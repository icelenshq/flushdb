use bytes::Bytes;
use flushdb_engine::{GetResult, MergeEntry, PageToken, RangeReadOptions, RangeReadResult};
use flushdb_proto::flushdb::v1 as proto;
use flushdb_types::{
    FlushError, FlushResult, IdempotencyToken, OrderedKey, MAX_ITEM_KEY_LEN, MAX_RECORD_ID_LEN,
};

use crate::NamespaceConfig;

#[derive(Debug, Clone)]
pub enum ParsedPredicate {
    MatchKeys { keys: Vec<Bytes> },
    MatchRange {
        start_key: Option<Bytes>,
        end_key: Option<Bytes>,
        start_inclusive: bool,
        end_inclusive: bool,
    },
    MatchAll,
}

// --- IdempotencyToken ---

pub fn proto_to_idempotency_token(
    proto: Option<proto::IdempotencyToken>,
) -> FlushResult<IdempotencyToken> {
    let Some(tok) = proto else {
        return Ok(IdempotencyToken::none());
    };

    if tok.generation_time == 0 && tok.token.is_empty() {
        return Ok(IdempotencyToken::none());
    }

    if tok.token.len() != 16 {
        return Err(FlushError::InvalidArgument {
            message: "idempotency token must be exactly 16 bytes".into(),
        });
    }

    let mut token_array = [0u8; 16];
    token_array.copy_from_slice(&tok.token);
    Ok(IdempotencyToken::from_parts(
        tok.generation_time,
        token_array,
    ))
}

pub fn idempotency_token_to_proto(token: &IdempotencyToken) -> proto::IdempotencyToken {
    proto::IdempotencyToken {
        generation_time: token.generation_time(),
        token: token.token_bytes().to_vec(),
    }
}

// --- OrderedKey ---

pub fn ordered_key_to_proto(key: &OrderedKey) -> proto::OrderedKey {
    proto::OrderedKey {
        timestamp_ms: key.timestamp_ms(),
        node_id: key.node_id() as u32,
        sequence: key.sequence() as u32,
    }
}

pub fn proto_to_ordered_key(proto: &proto::OrderedKey) -> FlushResult<OrderedKey> {
    if proto.node_id > 65535 {
        return Err(FlushError::InvalidArgument {
            message: format!(
                "node_id {} exceeds u16 maximum of 65535",
                proto.node_id
            ),
        });
    }
    if proto.sequence > 65535 {
        return Err(FlushError::InvalidArgument {
            message: format!(
                "sequence {} exceeds u16 maximum of 65535",
                proto.sequence
            ),
        });
    }
    Ok(OrderedKey::new(
        proto.timestamp_ms,
        proto.node_id as u16,
        proto.sequence as u16,
    ))
}

// --- Predicate ---

pub fn parse_predicate(predicate: Option<proto::Predicate>) -> FlushResult<ParsedPredicate> {
    let pred = predicate.ok_or_else(|| FlushError::InvalidArgument {
        message: "must specify a predicate".into(),
    })?;

    let inner = pred.predicate.ok_or_else(|| FlushError::InvalidArgument {
        message: "must specify a predicate".into(),
    })?;

    match inner {
        proto::predicate::Predicate::MatchKeys(mk) => {
            if mk.keys.is_empty() {
                return Err(FlushError::InvalidArgument {
                    message: "match_keys.keys: must specify at least one key".into(),
                });
            }
            let keys = mk.keys.into_iter().map(Bytes::from).collect();
            Ok(ParsedPredicate::MatchKeys { keys })
        }
        proto::predicate::Predicate::MatchRange(mr) => {
            let start_key = if mr.start_key.is_empty() {
                None
            } else {
                Some(Bytes::from(mr.start_key))
            };
            let end_key = if mr.end_key.is_empty() {
                None
            } else {
                Some(Bytes::from(mr.end_key))
            };

            if let (Some(ref s), Some(ref e)) = (&start_key, &end_key) {
                if s > e {
                    return Err(FlushError::InvalidArgument {
                        message: "match_range: start_key must be <= end_key".into(),
                    });
                }
            }

            Ok(ParsedPredicate::MatchRange {
                start_key,
                end_key,
                start_inclusive: mr.start_inclusive,
                end_inclusive: mr.end_inclusive,
            })
        }
        proto::predicate::Predicate::MatchAll(_) => Ok(ParsedPredicate::MatchAll),
    }
}

// --- Selection ---

pub fn parse_selection(
    selection: Option<proto::Selection>,
    config: &NamespaceConfig,
) -> FlushResult<(RangeReadOptions, bool)> {
    let Some(sel) = selection else {
        return Ok((
            RangeReadOptions {
                page_size_bytes: config.default_page_size_bytes as usize,
                item_limit: None,
                resume_from: None,
            },
            false,
        ));
    };

    let page_size_bytes = if sel.page_size_bytes == 0 {
        config.default_page_size_bytes as usize
    } else {
        (sel.page_size_bytes as usize).min(config.max_page_size_bytes as usize)
    };

    let item_limit = if sel.item_limit == 0 {
        None
    } else {
        Some(sel.item_limit as usize)
    };

    let resume_from = if sel.page_token.is_empty() {
        None
    } else {
        Some(PageToken::decode(&sel.page_token)?)
    };

    Ok((
        RangeReadOptions {
            page_size_bytes,
            item_limit,
            resume_from,
        },
        sel.exclude_values,
    ))
}

// --- Result formatting ---

pub fn get_result_to_proto_item(result: &GetResult) -> proto::Item {
    proto::Item {
        key: result.key.item_key().to_vec(),
        value: result.value.to_vec(),
        metadata: result.metadata.to_vec(),
        chunk: 0,
    }
}

pub fn merge_entry_to_proto_item(entry: &MergeEntry) -> proto::Item {
    proto::Item {
        key: entry.composite_key.item_key().to_vec(),
        value: entry.value.to_vec(),
        metadata: entry.metadata.to_vec(),
        chunk: 0,
    }
}

pub fn format_get_response(
    results: Vec<Option<GetResult>>,
    exclude_values: bool,
) -> Vec<proto::Item> {
    let mut items: Vec<proto::Item> = results
        .into_iter()
        .flatten()
        .map(|r| get_result_to_proto_item(&r))
        .collect();

    if exclude_values {
        for item in &mut items {
            item.value = Vec::new();
        }
    }

    items
}

pub fn format_scan_response(
    result: &RangeReadResult,
    exclude_values: bool,
) -> (Vec<proto::Item>, Vec<u8>) {
    let mut items: Vec<proto::Item> = result
        .entries
        .iter()
        .filter(|e| e.is_put())
        .map(merge_entry_to_proto_item)
        .collect();

    if exclude_values {
        for item in &mut items {
            item.value = Vec::new();
        }
    }

    let next_page_token_bytes = match &result.next_page_token {
        Some(token) => token.encode().to_vec(),
        None => Vec::new(),
    };

    (items, next_page_token_bytes)
}

// --- Error to gRPC Status ---

pub fn flush_error_to_status(err: FlushError) -> tonic::Status {
    match &err {
        FlushError::NotFound { .. } => tonic::Status::not_found(err.to_string()),
        FlushError::InvalidKey { .. } => tonic::Status::invalid_argument(err.to_string()),
        FlushError::KeyTooLong { .. } => tonic::Status::invalid_argument(err.to_string()),
        FlushError::InvalidArgument { .. } => tonic::Status::invalid_argument(err.to_string()),
        FlushError::DuplicateToken { .. } => tonic::Status::already_exists(err.to_string()),
        FlushError::PreconditionFailed { .. } => {
            tonic::Status::failed_precondition(err.to_string())
        }
        FlushError::ResourceExhausted { .. } => {
            tonic::Status::resource_exhausted(err.to_string())
        }
        FlushError::EpochFenced { .. } => tonic::Status::aborted(err.to_string()),
        FlushError::CorruptedData { .. } => tonic::Status::internal(err.to_string()),
        FlushError::CrcMismatch { .. } => tonic::Status::internal(err.to_string()),
        FlushError::Io(_) => tonic::Status::internal(err.to_string()),
    }
}

// --- Request validation ---

pub fn validate_namespace(namespace: &str) -> FlushResult<()> {
    if namespace.is_empty() {
        return Err(FlushError::InvalidArgument {
            message: "namespace: must not be empty".into(),
        });
    }
    Ok(())
}

pub fn validate_record_id(record_id: &str) -> FlushResult<()> {
    if record_id.is_empty() {
        return Err(FlushError::InvalidArgument {
            message: "record_id: must not be empty".into(),
        });
    }
    if record_id.contains('\0') {
        return Err(FlushError::InvalidArgument {
            message: "record_id: must not contain null bytes".into(),
        });
    }
    if record_id.len() > MAX_RECORD_ID_LEN {
        return Err(FlushError::InvalidArgument {
            message: format!(
                "record_id: length {} exceeds maximum of {MAX_RECORD_ID_LEN}",
                record_id.len()
            ),
        });
    }
    Ok(())
}

pub fn validate_items(items: &[proto::Item]) -> FlushResult<()> {
    if items.is_empty() {
        return Err(FlushError::InvalidArgument {
            message: "items: must not be empty".into(),
        });
    }
    for item in items {
        if item.key.len() > MAX_ITEM_KEY_LEN {
            return Err(FlushError::InvalidArgument {
                message: format!(
                    "item key length {} exceeds maximum of {MAX_ITEM_KEY_LEN}",
                    item.key.len()
                ),
            });
        }
    }
    Ok(())
}
