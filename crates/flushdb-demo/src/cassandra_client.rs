use std::sync::Arc;

use flushdb_proto::flushdb::v1::Item;
use scylla::batch::Batch;
use scylla::frame::types::Consistency;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::session::Session;
use scylla::SessionBuilder;

#[derive(Clone)]
pub struct CassandraClient {
    session: Arc<Session>,
    insert_stmt: PreparedStatement,
    get_all_stmt: PreparedStatement,
    delete_all_stmt: PreparedStatement,
    scan_range_stmt: PreparedStatement,
}

impl CassandraClient {
    pub async fn connect(
        addr: &str,
        keyspace: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let session: Session = SessionBuilder::new().known_node(addr).build().await?;

        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
                    {{'class': 'SimpleStrategy', 'replication_factor': 1}} \
                    AND DURABLE_WRITES = true",
                    keyspace
                ),
                &[],
            )
            .await?;

        session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {}.items (\
                    record_id text, \
                    item_key blob, \
                    item_value blob, \
                    metadata blob, \
                    PRIMARY KEY (record_id, item_key)) \
                    WITH compaction = {{'class': 'LeveledCompactionStrategy'}} \
                    AND compression = {{'class': 'LZ4Compressor'}} \
                    AND bloom_filter_fp_chance = 0.01",
                    keyspace
                ),
                &[],
            )
            .await?;

        let mut insert_stmt = session
            .prepare(format!(
                "INSERT INTO {}.items (record_id, item_key, item_value, metadata) VALUES (?, ?, ?, ?)",
                keyspace
            ))
            .await?;
        insert_stmt.set_consistency(Consistency::One);

        let mut get_all_stmt = session
            .prepare(format!(
                "SELECT item_key, item_value, metadata FROM {}.items WHERE record_id = ?",
                keyspace
            ))
            .await?;
        get_all_stmt.set_consistency(Consistency::One);

        let mut delete_all_stmt = session
            .prepare(format!(
                "DELETE FROM {}.items WHERE record_id = ?",
                keyspace
            ))
            .await?;
        delete_all_stmt.set_consistency(Consistency::One);

        let mut scan_range_stmt = session
            .prepare(format!(
                "SELECT item_key, item_value, metadata FROM {}.items \
                WHERE record_id = ? AND item_key >= ? AND item_key < ?",
                keyspace
            ))
            .await?;
        scan_range_stmt.set_consistency(Consistency::One);

        Ok(Self {
            session: Arc::new(session),
            insert_stmt,
            get_all_stmt,
            delete_all_stmt,
            scan_range_stmt,
        })
    }

    pub async fn put_product(
        &self,
        record_id: &str,
        items: Vec<Item>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut batch = Batch::default();
        type BatchRow = (String, Vec<u8>, Vec<u8>, Vec<u8>);
        let mut values: Vec<BatchRow> = Vec::with_capacity(items.len());
        for item in items {
            batch.append_statement(self.insert_stmt.clone());
            values.push((record_id.to_string(), item.key, item.value, item.metadata));
        }
        self.session.batch(&batch, &values).await?;
        Ok(())
    }

    pub async fn get_product_keys(
        &self,
        record_id: &str,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Item>, Box<dyn std::error::Error + Send + Sync>> {
        let result = self
            .session
            .execute_unpaged(&self.get_all_stmt, (record_id.to_string(),))
            .await?;
        let mut items = Vec::new();
        for row in result.rows_typed::<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)>()? {
            let (key, value, metadata) = row?;
            if keys.iter().any(|k| k == &key) {
                items.push(Item {
                    key,
                    value,
                    metadata: metadata.unwrap_or_default(),
                    chunk: 0,
                });
            }
        }
        Ok(items)
    }

    pub async fn delete_product(
        &self,
        record_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.session
            .execute_unpaged(&self.delete_all_stmt, (record_id.to_string(),))
            .await?;
        Ok(())
    }

    pub async fn scan_product_range(
        &self,
        record_id: &str,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<Vec<Item>, Box<dyn std::error::Error + Send + Sync>> {
        let result = self
            .session
            .execute_unpaged(
                &self.scan_range_stmt,
                (record_id.to_string(), start_key, end_key),
            )
            .await?;
        let mut items = Vec::new();
        for row in result.rows_typed::<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)>()? {
            let (key, value, metadata) = row?;
            items.push(Item {
                key,
                value,
                metadata: metadata.unwrap_or_default(),
                chunk: 0,
            });
        }
        Ok(items)
    }
}
