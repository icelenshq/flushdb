use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tonic::{Request, Response, Status};

use flushdb_proto::flushdb::v1 as proto;
use flushdb_proto::flushdb::v1::flush_db_server::FlushDb;
use flushdb_types::StorageBackend;

use crate::namespace_manager::NamespaceManager;
use crate::version_generator::VersionGenerator;

pub mod write;

pub struct FlushDbService<B: StorageBackend> {
    pub namespace_manager: Arc<NamespaceManager<B>>,
    pub version_generator: Arc<VersionGenerator>,
}

impl<B: StorageBackend> FlushDbService<B> {
    pub fn new(
        namespace_manager: Arc<NamespaceManager<B>>,
        version_generator: Arc<VersionGenerator>,
    ) -> Self {
        Self {
            namespace_manager,
            version_generator,
        }
    }
}

#[tonic::async_trait]
impl<B: StorageBackend + Clone + Send + Sync + 'static> FlushDb for FlushDbService<B> {
    type ScanItemsStream =
        Pin<Box<dyn Stream<Item = Result<proto::ScanItemsResponse, Status>> + Send>>;

    async fn put_items(
        &self,
        request: Request<proto::PutItemsRequest>,
    ) -> Result<Response<proto::PutItemsResponse>, Status> {
        write::handle_put_items(self, request).await
    }

    async fn delete_items(
        &self,
        request: Request<proto::DeleteItemsRequest>,
    ) -> Result<Response<proto::DeleteItemsResponse>, Status> {
        write::handle_delete_items(self, request).await
    }

    async fn get_items(
        &self,
        _request: Request<proto::GetItemsRequest>,
    ) -> Result<Response<proto::GetItemsResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn scan_items(
        &self,
        _request: Request<proto::ScanItemsRequest>,
    ) -> Result<Response<Self::ScanItemsStream>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }
}
