use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

use flushdb_types::{FlushError, FlushResult, StorageBackend};

const MULTIPART_THRESHOLD: usize = 16 * 1024 * 1024;
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct S3StorageBackend {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix_count: u32,
}

impl S3StorageBackend {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
            prefix_count: 128,
        }
    }

    pub fn with_prefix_count(
        client: aws_sdk_s3::Client,
        bucket: String,
        prefix_count: u32,
    ) -> FlushResult<Self> {
        if bucket.is_empty() {
            return Err(FlushError::InvalidArgument {
                message: "bucket name must not be empty".to_string(),
            });
        }
        if prefix_count == 0 {
            return Err(FlushError::InvalidArgument {
                message: "prefix_count must be greater than 0".to_string(),
            });
        }
        if !prefix_count.is_power_of_two() {
            return Err(FlushError::InvalidArgument {
                message: format!("prefix_count must be a power of 2, got {}", prefix_count),
            });
        }
        Ok(Self {
            client,
            bucket,
            prefix_count,
        })
    }

    pub async fn from_env(bucket: String) -> FlushResult<Self> {
        if bucket.is_empty() {
            return Err(FlushError::InvalidArgument {
                message: "bucket name must not be empty".to_string(),
            });
        }
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = if std::env::var("AWS_ENDPOINT_URL").is_ok() {
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            aws_sdk_s3::Client::from_conf(s3_config)
        } else {
            aws_sdk_s3::Client::new(&shared_config)
        };
        Ok(Self {
            client,
            bucket,
            prefix_count: 128,
        })
    }

    pub fn shard_key(&self, logical_key: &str) -> String {
        let hash = crc32fast::hash(logical_key.as_bytes());
        let shard = hash % self.prefix_count;
        format!("{:03}/{}", shard, logical_key)
    }

    async fn put_simple(&self, sharded_key: &str, value: Bytes) -> FlushResult<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(sharded_key)
            .body(ByteStream::from(value))
            .send()
            .await
            .map_err(|err| map_sdk_error_generic(&err))?;
        Ok(())
    }

    async fn put_multipart(&self, sharded_key: &str, value: Bytes) -> FlushResult<()> {
        let create_resp = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(sharded_key)
            .send()
            .await
            .map_err(|err| map_sdk_error_generic(&err))?;

        let upload_id = create_resp
            .upload_id()
            .ok_or_else(|| {
                FlushError::Io(std::io::Error::other(
                    "S3 CreateMultipartUpload returned no upload_id",
                ))
            })?
            .to_string();

        let mut completed_parts = Vec::new();
        let total_len = value.len();
        let mut offset = 0usize;
        let mut part_number: i32 = 1;

        while offset < total_len {
            let end = std::cmp::min(offset + MULTIPART_PART_SIZE, total_len);
            let chunk = value.slice(offset..end);

            let upload_result = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(sharded_key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(chunk))
                .send()
                .await;

            match upload_result {
                Ok(resp) => {
                    let etag = resp.e_tag().unwrap_or_default().to_string();
                    completed_parts.push(
                        CompletedPart::builder()
                            .e_tag(etag)
                            .part_number(part_number)
                            .build(),
                    );
                }
                Err(err) => {
                    let _ = self
                        .client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(sharded_key)
                        .upload_id(&upload_id)
                        .send()
                        .await;
                    return Err(map_sdk_error_generic(&err));
                }
            }

            offset = end;
            part_number += 1;
        }

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(sharded_key)
            .upload_id(&upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|err| {
                // Best-effort abort; we cannot await in map_err so we drop the future.
                // The upload will be cleaned up by S3 lifecycle rules.
                drop(
                    self.client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(sharded_key)
                        .upload_id(&upload_id)
                        .send(),
                );
                map_sdk_error_generic(&err)
            })?;

        Ok(())
    }

    async fn list_single_shard(
        &self,
        shard_index: u32,
        logical_prefix: &str,
    ) -> FlushResult<Vec<String>> {
        let s3_prefix = format!("{:03}/{}", shard_index, logical_prefix);
        let strip_prefix = format!("{:03}/", shard_index);
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&s3_prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let resp = request
                .send()
                .await
                .map_err(|err| map_sdk_error_generic(&err))?;

            for object in resp.contents() {
                if let Some(key) = object.key() {
                    if let Some(stripped) = key.strip_prefix(&strip_prefix) {
                        keys.push(stripped.to_string());
                    }
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                if continuation_token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(keys)
    }
}

#[async_trait]
impl StorageBackend for S3StorageBackend {
    async fn put(&self, key: &str, value: Bytes) -> FlushResult<()> {
        let sharded_key = self.shard_key(key);
        if value.len() <= MULTIPART_THRESHOLD {
            self.put_simple(&sharded_key, value).await
        } else {
            self.put_multipart(&sharded_key, value).await
        }
    }

    async fn get(&self, key: &str) -> FlushResult<Bytes> {
        let sharded_key = self.shard_key(key);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&sharded_key)
            .send()
            .await
            .map_err(|err| map_get_error(key, &err))?;

        let body = resp
            .body
            .collect()
            .await
            .map_err(|err| FlushError::Io(std::io::Error::other(err.to_string())))?
            .into_bytes();

        Ok(body)
    }

    async fn get_range(&self, key: &str, offset: u64, length: u64) -> FlushResult<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        let sharded_key = self.shard_key(key);
        let range = format!("bytes={}-{}", offset, offset + length - 1);

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&sharded_key)
            .range(range)
            .send()
            .await
            .map_err(|err| map_get_range_error(key, &err))?;

        let body = resp
            .body
            .collect()
            .await
            .map_err(|err| FlushError::Io(std::io::Error::other(err.to_string())))?
            .into_bytes();

        Ok(body)
    }

    async fn delete(&self, key: &str) -> FlushResult<()> {
        let sharded_key = self.shard_key(key);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&sharded_key)
            .send()
            .await
            .map_err(|err| map_sdk_error_generic(&err))?;
        Ok(())
    }

    async fn conditional_put(&self, key: &str, value: Bytes) -> FlushResult<()> {
        let sharded_key = self.shard_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&sharded_key)
            .body(ByteStream::from(value))
            .if_none_match("*")
            .send()
            .await
            .map_err(|err| map_conditional_put_error(&err))?;
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> FlushResult<Vec<String>> {
        let prefix_owned = prefix.to_string();
        let shard_indices: Vec<u32> = (0..self.prefix_count).collect();

        let results: Vec<FlushResult<Vec<String>>> = stream::iter(shard_indices)
            .map(|shard_index| {
                let prefix_ref = &prefix_owned;
                async move { self.list_single_shard(shard_index, prefix_ref).await }
            })
            .buffer_unordered(50)
            .collect()
            .await;

        let mut all_keys = Vec::new();
        for result in results {
            all_keys.extend(result?);
        }
        all_keys.sort();
        Ok(all_keys)
    }
}

fn map_sdk_error_generic<E: std::fmt::Debug>(err: &E) -> FlushError {
    FlushError::Io(std::io::Error::other(format!("{err:?}")))
}

fn map_get_error(
    key: &str,
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> FlushError {
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::get_object::GetObjectError;

    match err {
        SdkError::ServiceError(service_err) => match service_err.err() {
            GetObjectError::NoSuchKey(_) => FlushError::NotFound {
                key: key.to_string(),
            },
            _ => FlushError::Io(std::io::Error::other(err.to_string())),
        },
        _ => FlushError::Io(std::io::Error::other(err.to_string())),
    }
}

fn map_get_range_error(
    key: &str,
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> FlushError {
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::get_object::GetObjectError;

    match err {
        SdkError::ServiceError(service_err) => match service_err.err() {
            GetObjectError::NoSuchKey(_) => FlushError::NotFound {
                key: key.to_string(),
            },
            _ => {
                let status = service_err.raw().status().as_u16();
                if status == 416 {
                    FlushError::InvalidArgument {
                        message: "range extends beyond object".to_string(),
                    }
                } else {
                    FlushError::Io(std::io::Error::other(err.to_string()))
                }
            }
        },
        _ => FlushError::Io(std::io::Error::other(err.to_string())),
    }
}

fn map_conditional_put_error(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> FlushError {
    use aws_sdk_s3::error::SdkError;

    match err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            if status == 412 {
                FlushError::PreconditionFailed {
                    message: "key already exists".to_string(),
                }
            } else {
                FlushError::Io(std::io::Error::other(err.to_string()))
            }
        }
        _ => FlushError::Io(std::io::Error::other(err.to_string())),
    }
}
