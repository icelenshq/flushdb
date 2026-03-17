use flushdb_proto::flushdb::v1::flush_db_client::FlushDbClient;
use flushdb_proto::flushdb::v1::{
    DeleteItemsRequest, GetItemsRequest, GetItemsResponse, IdempotencyToken, Item, MatchKeys,
    MatchRange, Predicate, PutItemsRequest, PutItemsResponse, ScanItemsRequest, ScanItemsResponse,
};
use tonic::transport::Channel;

pub struct DemoClient {
    inner: FlushDbClient<Channel>,
}

fn bypass_token() -> Option<IdempotencyToken> {
    Some(IdempotencyToken {
        generation_time: 0,
        token: vec![0u8; 16],
    })
}

impl DemoClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut last_err = None;
        for attempt in 0..10 {
            match FlushDbClient::connect(addr.to_string()).await {
                Ok(client) => return Ok(Self { inner: client }),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 9 {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
        }
        Err(Box::new(last_err.expect("at least one attempt")))
    }

    pub async fn put_product(
        &mut self,
        namespace: &str,
        record_id: &str,
        items: Vec<Item>,
    ) -> Result<PutItemsResponse, tonic::Status> {
        let req = PutItemsRequest {
            idempotency_token: bypass_token(),
            namespace: namespace.to_string(),
            id: record_id.to_string(),
            items,
        };
        self.inner.put_items(req).await.map(|r| r.into_inner())
    }

    pub async fn get_product_keys(
        &mut self,
        namespace: &str,
        record_id: &str,
        keys: Vec<Vec<u8>>,
    ) -> Result<GetItemsResponse, tonic::Status> {
        let req = GetItemsRequest {
            namespace: namespace.to_string(),
            id: record_id.to_string(),
            predicate: Some(Predicate {
                predicate: Some(
                    flushdb_proto::flushdb::v1::predicate::Predicate::MatchKeys(MatchKeys { keys }),
                ),
            }),
            selection: None,
            signals: Default::default(),
        };
        self.inner.get_items(req).await.map(|r| r.into_inner())
    }

    pub async fn get_product_all(
        &mut self,
        namespace: &str,
        record_id: &str,
    ) -> Result<GetItemsResponse, tonic::Status> {
        let req = GetItemsRequest {
            namespace: namespace.to_string(),
            id: record_id.to_string(),
            predicate: Some(Predicate {
                predicate: Some(
                    flushdb_proto::flushdb::v1::predicate::Predicate::MatchAll(true),
                ),
            }),
            selection: None,
            signals: Default::default(),
        };
        self.inner.get_items(req).await.map(|r| r.into_inner())
    }

    pub async fn scan_product_range(
        &mut self,
        namespace: &str,
        record_id: &str,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<Vec<ScanItemsResponse>, tonic::Status> {
        let req = ScanItemsRequest {
            namespace: namespace.to_string(),
            id: record_id.to_string(),
            predicate: Some(Predicate {
                predicate: Some(
                    flushdb_proto::flushdb::v1::predicate::Predicate::MatchRange(MatchRange {
                        start_key,
                        end_key,
                        start_inclusive: true,
                        end_inclusive: false,
                    }),
                ),
            }),
            signals: Default::default(),
        };
        let mut stream = self.inner.scan_items(req).await?.into_inner();
        let mut responses = Vec::new();
        while let Some(resp) = stream.message().await? {
            responses.push(resp);
        }
        Ok(responses)
    }

    pub async fn delete_product(
        &mut self,
        namespace: &str,
        record_id: &str,
    ) -> Result<(), tonic::Status> {
        let req = DeleteItemsRequest {
            idempotency_token: bypass_token(),
            namespace: namespace.to_string(),
            id: record_id.to_string(),
            predicate: Some(Predicate {
                predicate: Some(
                    flushdb_proto::flushdb::v1::predicate::Predicate::MatchAll(true),
                ),
            }),
        };
        self.inner.delete_items(req).await?;
        Ok(())
    }
}
