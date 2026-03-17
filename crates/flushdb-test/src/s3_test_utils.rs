use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use flushdb_server::S3StorageBackend;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers::GenericImage;
use testcontainers::ImageExt;

static MINIO_PORT: OnceLock<u16> = OnceLock::new();
static CONTAINER: OnceLock<tokio::sync::OnceCell<ContainerAsync<GenericImage>>> = OnceLock::new();
static EXIT_CLEANUP_REGISTERED: AtomicBool = AtomicBool::new(false);

const TEST_BUCKET: &str = "flushdb-test";

// MinIO added if-none-match (conditional writes) support in RELEASE.2024-02-06.
// Newer MinIO versions (2024+) write startup logs to stderr instead of stdout,
// so we use GenericImage with message_on_stderr instead of the testcontainers-modules MinIO module.
const MINIO_TAG: &str = "RELEASE.2025-02-28T09-55-16Z";

const MINIO_CONTAINER_NAME: &str = "flushdb-test-minio";

/// Remove the named MinIO container if it exists (from a previous test run or crash).
fn remove_named_container() {
    let _ = Command::new("docker")
        .args(["rm", "-f", MINIO_CONTAINER_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Register an `atexit` handler that removes the MinIO container when the
/// test process exits normally. This covers the case where `ContainerAsync`
/// lives in a static and its `Drop` is never called.
///
/// For abnormal exits (SIGKILL, crash), the startup cleanup in `ensure_minio`
/// handles stale containers on the next run.
fn register_exit_cleanup() {
    if EXIT_CLEANUP_REGISTERED.swap(true, Ordering::SeqCst) {
        return;
    }

    extern "C" fn on_exit() {
        let _ = Command::new("docker")
            .args(["rm", "-f", MINIO_CONTAINER_NAME])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    // SAFETY: `on_exit` is a valid extern "C" function that does not panic and
    // only calls `std::process::Command` (which is safe and available during
    // atexit). This is the only way to clean up a container stored in a static
    // without restructuring the entire test harness.
    #[allow(unsafe_code)]
    unsafe {
        libc::atexit(on_exit);
    }
}

pub async fn ensure_minio() -> u16 {
    let cell = CONTAINER.get_or_init(tokio::sync::OnceCell::new);
    let container = cell
        .get_or_init(|| async {
            // Remove stale container from a previous test run (crash, SIGKILL, etc.)
            remove_named_container();

            let container = GenericImage::new("minio/minio", MINIO_TAG)
                .with_exposed_port(9000.tcp())
                .with_wait_for(WaitFor::message_on_stderr("API:"))
                .with_container_name(MINIO_CONTAINER_NAME)
                .with_env_var("MINIO_CONSOLE_ADDRESS", ":9001")
                .with_cmd(["server", "/data"])
                .start()
                .await
                .expect("Failed to start MinIO");
            let port = container
                .get_host_port_ipv4(9000)
                .await
                .expect("Failed to get MinIO port");
            MINIO_PORT.get_or_init(|| port);

            // Ensure container is removed on normal process exit
            register_exit_cleanup();

            let client = create_s3_client(port).await;
            let _ = client
                .create_bucket()
                .bucket(TEST_BUCKET)
                .send()
                .await;

            container
        })
        .await;

    container
        .get_host_port_ipv4(9000)
        .await
        .expect("Failed to get port")
}

async fn create_s3_client(port: u16) -> aws_sdk_s3::Client {
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .endpoint_url(format!("http://127.0.0.1:{}", port))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

pub async fn create_test_s3_backend() -> S3StorageBackend {
    let port = ensure_minio().await;
    let client = create_s3_client(port).await;
    S3StorageBackend::new(client, TEST_BUCKET.to_string())
}

pub fn test_prefix(test_name: &str) -> String {
    format!("test-{}-{}/", test_name, uuid::Uuid::new_v4())
}

pub async fn cleanup_test_prefix(backend: &S3StorageBackend, prefix: &str) {
    use flushdb_types::StorageBackend;
    if let Ok(keys) = backend.list_prefix(prefix).await {
        for key in keys {
            let _ = backend.delete(&key).await;
        }
    }
}
