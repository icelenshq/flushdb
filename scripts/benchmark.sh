#!/usr/bin/env bash
# Sequential Docker-only benchmark:
#   1. Optional smoke checks run before measurement.
#   2. Each scale step runs flushdb alone, then Cassandra alone.
#   3. The benchmark client (flushdb-demo) also runs inside Docker.
#   4. Steps scale seed size and product range to model real-world growth.
#
# Usage:
#   ./scripts/benchmark.sh [options]
#
# Options:
#   --duration SECS      Measurement window per backend (default: 30)
#   --warmup SECS        Warmup window per backend (default: 5)
#   --concurrency N      Worker count (default: 8)
#   --output-dir DIR     Directory for JSON/log artifacts (default: /tmp/flushdb_bench)
#   --log-file PATH      Log file path (default: OUTPUT_DIR/benchmark.log)
#   --from-step N        First scale step to run, 1-indexed (default: 1)
#   --to-step N          Last scale step to run, inclusive (default: 10)
#   --skip-smoke         Skip the preflight smoke seed/verify pass
#
# Example:
#   ./scripts/benchmark.sh --duration 30 --warmup 5

set -euo pipefail

DURATION=30
WARMUP=5
CONCURRENCY=8
OUTPUT_DIR=/tmp/flushdb_bench
LOG_FILE=""
FROM_STEP=1
TO_STEP=10
RUN_SMOKE=1
WRITE_RATIO=0.15
UPDATE_RATIO=0.15
DELETE_RATIO=0.05
SCAN_RATIO=0.05

SEED_STEPS=(1000 2500 5000 10000 15000 25000 40000 50000 75000 100000)
RANGE_STEPS=(100000 250000 500000 1000000 1500000 2500000 4000000 5000000 7500000 10000000)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)      DURATION="$2";    shift 2 ;;
        --warmup)        WARMUP="$2";      shift 2 ;;
        --concurrency)   CONCURRENCY="$2"; shift 2 ;;
        --output-dir)    OUTPUT_DIR="$2";  shift 2 ;;
        --log-file)      LOG_FILE="$2";    shift 2 ;;
        --from-step)     FROM_STEP="$2";   shift 2 ;;
        --to-step)       TO_STEP="$2";     shift 2 ;;
        --skip-smoke)    RUN_SMOKE=0;      shift 1 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

if [[ -z "$LOG_FILE" ]]; then
    LOG_FILE="$OUTPUT_DIR/benchmark.log"
fi

: >"$LOG_FILE"

TOTAL_STEPS="${#SEED_STEPS[@]}"

if [[ "$TOTAL_STEPS" -ne "${#RANGE_STEPS[@]}" ]]; then
    echo "Scale step arrays are mismatched" >&2
    exit 1
fi

if (( FROM_STEP < 1 || TO_STEP > TOTAL_STEPS || FROM_STEP > TO_STEP )); then
    echo "--from-step/--to-step must be within 1..$TOTAL_STEPS and ordered" >&2
    exit 1
fi

log() {
    local ts
    ts="$(date '+%Y-%m-%dT%H:%M:%S')"
    echo "[$ts] [benchmark] $*" | tee -a "$LOG_FILE"
}

wait_tcp() {
    local host="$1" port="$2" label="$3"
    log "Waiting for $label on $host:$port..."
    local i=0
    until bash -c ">/dev/tcp/$host/$port" 2>/dev/null; do
        ((i++))
        if [[ $i -ge 120 ]]; then
            log "ERROR: timed out waiting for $label"
            exit 1
        fi
        sleep 1
    done
    log "$label is ready"
}

wait_service_healthy() {
    local service="$1" label="$2"
    log "Waiting for $label health=healthy..."
    local i=0
    while true; do
        local health
        health="$(docker compose ps --format json "$service" 2>/dev/null \
            | python3 -c "import sys, json; rows=json.load(sys.stdin); row=rows[0] if isinstance(rows, list) else rows; print(row.get('Health', 'missing'))" 2>/dev/null || echo "missing")"
        if [[ "$health" == "healthy" ]]; then
            log "$label health is healthy"
            return 0
        fi
        ((i++))
        if [[ $i -ge 120 ]]; then
            log "ERROR: timed out waiting for $label health (last=$health)"
            exit 1
        fi
        sleep 1
    done
}

check_container_running() {
    local service="$1"
    local state
    state="$(docker compose ps --format json "$service" 2>/dev/null \
        | python3 -c "import sys, json; rows=json.load(sys.stdin); print(rows[0]['State'] if isinstance(rows, list) else rows['State'])" 2>/dev/null || echo "missing")"
    if [[ "$state" != "running" ]]; then
        log "ERROR: container '$service' is not running (state=$state). Check 'docker compose logs $service'."
        exit 1
    fi
    log "Container '$service' confirmed running in Docker (state=$state)"
}

stop_profile() {
    local profile="$1"
    log "Stopping profile=$profile and removing volumes..."
    docker compose --profile "$profile" down -v --remove-orphans >>"$LOG_FILE" 2>&1 || true
}

run_demo() {
    docker compose run --rm --no-deps \
        -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
        -e CASSANDRA_ADDR=cassandra:9042 \
        -v "$OUTPUT_DIR:/bench" \
        flushdb-demo "$@"
}

run_and_capture() {
    local phase_log="$1"
    shift
    : >"$phase_log"
    (
        set -o pipefail
        "$@" 2>&1 | tee "$phase_log"
    )
    cat "$phase_log" >>"$LOG_FILE"
}

write_manifest() {
    cat >"$OUTPUT_DIR/scale_manifest.csv" <<EOF
step,seed_products,product_range,duration_secs,warmup_secs,concurrency
EOF
    local idx
    for idx in $(seq "$FROM_STEP" "$TO_STEP"); do
        printf "%d,%d,%d,%d,%d,%d\n" \
            "$idx" \
            "${SEED_STEPS[$((idx - 1))]}" \
            "${RANGE_STEPS[$((idx - 1))]}" \
            "$DURATION" \
            "$WARMUP" \
            "$CONCURRENCY" >>"$OUTPUT_DIR/scale_manifest.csv"
    done
}

run_smoke_checks() {
    log "Running smoke seed/verify pass before measured benchmark..."
    stop_profile cassandra
    stop_profile flushdb

    log "Starting flushdb profile for smoke checks..."
    docker compose --profile flushdb up -d >>"$LOG_FILE" 2>&1
    wait_service_healthy minio minio
    wait_tcp 127.0.0.1 50051 flushdb-server
    check_container_running flushdb-server

    run_and_capture "$OUTPUT_DIR/smoke_seed.log" \
        run_demo seed --products 100 --concurrency 2
    run_and_capture "$OUTPUT_DIR/smoke_verify.log" \
        run_demo verify --sample-size 10 --product-range 100

    stop_profile flushdb
    sleep 2
    log "Smoke checks completed"
}

log "Log file: $LOG_FILE"
log "Benchmark client: flushdb-demo container (docker compose run --no-deps)"
log "Backend resource limits: flushdb-server 2 CPU / 1 GiB, cassandra 2 CPU / 1 GiB, minio 1 CPU / 512 MiB"
log "Scale ladder: ${TOTAL_STEPS} predefined steps, step range ${FROM_STEP}-${TO_STEP}"

write_manifest

log "Building benchmark images (flushdb-server + flushdb-demo)..."
cd "$REPO_ROOT"
docker compose build flushdb-server flushdb-demo >>"$LOG_FILE" 2>&1
log "Docker image build complete"

if (( RUN_SMOKE )); then
    run_smoke_checks
fi

for step in $(seq "$FROM_STEP" "$TO_STEP"); do
    seed_products="${SEED_STEPS[$((step - 1))]}"
    product_range="${RANGE_STEPS[$((step - 1))]}"
    step_name="$(printf 'step_%02d' "$step")"

    flushdb_json="$OUTPUT_DIR/${step_name}_flushdb.json"
    flushdb_log="$OUTPUT_DIR/${step_name}_flushdb.log"
    cassandra_log="$OUTPUT_DIR/${step_name}_cassandra.log"

    log "========================================================"
    log "Scale step $step / $TOTAL_STEPS  |  seed_products=$seed_products  product_range=$product_range"
    log "========================================================"

    stop_profile cassandra
    stop_profile flushdb

    log "--- ${step_name}: flushdb phase ---"
    docker compose --profile flushdb up -d >>"$LOG_FILE" 2>&1
    wait_service_healthy minio minio
    wait_tcp 127.0.0.1 50051 flushdb-server
    check_container_running flushdb-server

    run_and_capture "$flushdb_log" \
        run_demo bench compare \
            --phase flushdb \
            --output "/bench/${step_name}_flushdb.json" \
            --duration "$DURATION" \
            --warmup-secs "$WARMUP" \
            --concurrency "$CONCURRENCY" \
            --seed-products "$seed_products" \
            --product-range "$product_range" \
            --write-ratio "$WRITE_RATIO" \
            --update-ratio "$UPDATE_RATIO" \
            --delete-ratio "$DELETE_RATIO" \
            --scan-ratio "$SCAN_RATIO"

    stop_profile flushdb
    sleep 2

    log "--- ${step_name}: cassandra phase ---"
    docker compose --profile cassandra up -d >>"$LOG_FILE" 2>&1
    wait_service_healthy cassandra cassandra
    check_container_running cassandra

    run_and_capture "$cassandra_log" \
        run_demo bench compare \
            --phase cassandra \
            --flushdb-results "/bench/${step_name}_flushdb.json" \
            --duration "$DURATION" \
            --warmup-secs "$WARMUP" \
            --concurrency "$CONCURRENCY" \
            --seed-products "$seed_products" \
            --product-range "$product_range" \
            --write-ratio "$WRITE_RATIO" \
            --update-ratio "$UPDATE_RATIO" \
            --delete-ratio "$DELETE_RATIO" \
            --scan-ratio "$SCAN_RATIO" \
            --cassandra-addr cassandra:9042

    stop_profile cassandra
    sleep 2

    log "Completed ${step_name}"
done

log "All requested scale steps complete. Artifacts in $OUTPUT_DIR  |  Log: $LOG_FILE"
