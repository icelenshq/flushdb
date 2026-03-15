use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tonic::{Request, Response, Status};

use flushdb_engine::RangeReadOptions;
use flushdb_proto::flushdb::v1 as proto;
use flushdb_types::StorageBackend;

use crate::conversions::{
    flush_error_to_status, format_get_response, format_scan_response, parse_predicate,
    parse_selection, validate_namespace, validate_record_id, ParsedPredicate,
};
use crate::namespace_manager::NamespaceManager;

use super::FlushDbService;

pub async fn handle_get_items<B: StorageBackend + Clone + 'static>(
    service: &FlushDbService<B>,
    request: Request<proto::GetItemsRequest>,
) -> Result<Response<proto::GetItemsResponse>, Status> {
    let req = request.into_inner();

    validate_namespace(&req.namespace).map_err(flush_error_to_status)?;
    validate_record_id(&req.id).map_err(flush_error_to_status)?;

    let predicate = parse_predicate(req.predicate).map_err(flush_error_to_status)?;

    let config = service
        .namespace_manager
        .get_namespace_config(&req.namespace)
        .map_err(flush_error_to_status)?;

    let (options, exclude_values) =
        parse_selection(req.selection, &config).map_err(flush_error_to_status)?;

    let (items, next_page_token) = match predicate {
        ParsedPredicate::MatchKeys { keys } => {
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            let results = service
                .namespace_manager
                .multi_get(&req.namespace, &req.id, &key_refs)
                .await
                .map_err(flush_error_to_status)?;
            let items = format_get_response(results, exclude_values);
            (items, Vec::new())
        }
        ParsedPredicate::MatchRange {
            start_key, end_key, ..
        } => {
            let start = start_key.as_deref();
            let end = end_key.as_deref();
            let result = service
                .namespace_manager
                .scan(&req.namespace, &req.id, start, end, options)
                .await
                .map_err(flush_error_to_status)?;
            format_scan_response(&result, exclude_values)
        }
        ParsedPredicate::MatchAll => {
            let result = service
                .namespace_manager
                .scan(&req.namespace, &req.id, None, None, options)
                .await
                .map_err(flush_error_to_status)?;
            format_scan_response(&result, exclude_values)
        }
    };

    Ok(Response::new(proto::GetItemsResponse {
        items,
        next_page_token,
    }))
}

pub async fn handle_scan_items<B: StorageBackend + Clone + Send + Sync + 'static>(
    service: &FlushDbService<B>,
    request: Request<proto::ScanItemsRequest>,
) -> Result<
    Response<Pin<Box<dyn Stream<Item = Result<proto::ScanItemsResponse, Status>> + Send>>>,
    Status,
> {
    let req = request.into_inner();

    validate_namespace(&req.namespace).map_err(flush_error_to_status)?;
    validate_record_id(&req.id).map_err(flush_error_to_status)?;

    let predicate = parse_predicate(req.predicate).map_err(flush_error_to_status)?;

    let config = service
        .namespace_manager
        .get_namespace_config(&req.namespace)
        .map_err(flush_error_to_status)?;

    let default_page_size = config.default_page_size_bytes as usize;

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let manager = service.namespace_manager.clone();
    let namespace = req.namespace;
    let record_id = req.id;

    tokio::spawn(async move {
        match predicate {
            ParsedPredicate::MatchKeys { keys } => {
                scan_match_keys(&manager, &namespace, &record_id, &keys, &tx).await;
            }
            ParsedPredicate::MatchRange {
                start_key, end_key, ..
            } => {
                scan_range(
                    &manager,
                    &namespace,
                    &record_id,
                    start_key.as_deref(),
                    end_key.as_deref(),
                    default_page_size,
                    &tx,
                )
                .await;
            }
            ParsedPredicate::MatchAll => {
                scan_range(
                    &manager,
                    &namespace,
                    &record_id,
                    None,
                    None,
                    default_page_size,
                    &tx,
                )
                .await;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Response::new(Box::pin(stream)))
}

async fn scan_match_keys<B: StorageBackend + Clone + 'static>(
    manager: &Arc<NamespaceManager<B>>,
    namespace: &str,
    record_id: &str,
    keys: &[bytes::Bytes],
    tx: &tokio::sync::mpsc::Sender<Result<proto::ScanItemsResponse, Status>>,
) {
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
    let results = match manager.multi_get(namespace, record_id, &key_refs).await {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.send(Err(flush_error_to_status(e))).await;
            return;
        }
    };

    let items = format_get_response(results, false);
    if !items.is_empty() {
        let response = proto::ScanItemsResponse { items };
        let _ = tx.send(Ok(response)).await;
    }
}

async fn scan_range<B: StorageBackend + Clone + 'static>(
    manager: &Arc<NamespaceManager<B>>,
    namespace: &str,
    record_id: &str,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    default_page_size: usize,
    tx: &tokio::sync::mpsc::Sender<Result<proto::ScanItemsResponse, Status>>,
) {
    let mut resume_from = None;

    loop {
        let options = RangeReadOptions {
            page_size_bytes: default_page_size,
            item_limit: None,
            resume_from: resume_from.take(),
        };

        let result = match manager
            .scan(namespace, record_id, start_key, end_key, options)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.send(Err(flush_error_to_status(e))).await;
                return;
            }
        };

        let (items, _token_bytes) = format_scan_response(&result, false);

        if items.is_empty() && result.next_page_token.is_none() {
            break;
        }

        if !items.is_empty() {
            let response = proto::ScanItemsResponse { items };
            if tx.send(Ok(response)).await.is_err() {
                return;
            }
        }

        match result.next_page_token {
            Some(token) => resume_from = Some(token),
            None => break,
        }
    }
}
