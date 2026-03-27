use bytes::Bytes;
use tonic::{Request, Response, Status};

use flushdb_engine::PutBatchItem;
use flushdb_proto::flushdb::v1 as proto;
use flushdb_types::StorageBackend;

use crate::conversions::{
    flush_error_to_status, ordered_key_to_proto, parse_predicate, proto_to_idempotency_token,
    validate_items, validate_namespace, validate_record_id, ParsedPredicate,
};

use super::FlushDbService;

pub async fn handle_put_items<B: StorageBackend + Clone + 'static>(
    service: &FlushDbService<B>,
    request: Request<proto::PutItemsRequest>,
) -> Result<Response<proto::PutItemsResponse>, Status> {
    let req = request.into_inner();
    let proto::PutItemsRequest {
        idempotency_token,
        namespace,
        id,
        items,
    } = req;

    validate_namespace(&namespace).map_err(flush_error_to_status)?;
    validate_record_id(&id).map_err(flush_error_to_status)?;
    validate_items(&items).map_err(flush_error_to_status)?;

    let base_token =
        proto_to_idempotency_token(idempotency_token).map_err(flush_error_to_status)?;

    let items = items
        .into_iter()
        .enumerate()
        .map(|(index, item)| PutBatchItem {
            item_key: Bytes::from(item.key),
            value: Bytes::from(item.value),
            metadata: Bytes::from(item.metadata),
            idempotency_token: base_token.derive_for_index(index as u32),
        })
        .collect();

    service
        .namespace_manager
        .put_batch(&namespace, &id, items)
        .await
        .map_err(flush_error_to_status)?;

    let version = service.version_generator.next_version();

    Ok(Response::new(proto::PutItemsResponse {
        version: Some(ordered_key_to_proto(&version)),
    }))
}

pub async fn handle_delete_items<B: StorageBackend + Clone + 'static>(
    service: &FlushDbService<B>,
    request: Request<proto::DeleteItemsRequest>,
) -> Result<Response<proto::DeleteItemsResponse>, Status> {
    let req = request.into_inner();

    validate_namespace(&req.namespace).map_err(flush_error_to_status)?;
    validate_record_id(&req.id).map_err(flush_error_to_status)?;

    let predicate = parse_predicate(req.predicate).map_err(flush_error_to_status)?;

    match predicate {
        ParsedPredicate::MatchAll => {
            service
                .namespace_manager
                .delete_range(&req.namespace, &req.id, b"", b"")
                .await
                .map_err(flush_error_to_status)?;
        }
        ParsedPredicate::MatchRange {
            start_key, end_key, ..
        } => {
            let empty = Bytes::new();
            let start = start_key.as_ref().unwrap_or(&empty);
            let end = end_key.as_ref().unwrap_or(&empty);
            service
                .namespace_manager
                .delete_range(&req.namespace, &req.id, start, end)
                .await
                .map_err(flush_error_to_status)?;
        }
        ParsedPredicate::MatchKeys { keys } => {
            for key in &keys {
                service
                    .namespace_manager
                    .delete(&req.namespace, &req.id, key)
                    .await
                    .map_err(flush_error_to_status)?;
            }
        }
    }

    let version = service.version_generator.next_version();

    Ok(Response::new(proto::DeleteItemsResponse {
        version: Some(ordered_key_to_proto(&version)),
    }))
}
