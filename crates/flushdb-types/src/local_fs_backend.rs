use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::{FlushError, FlushResult};
use crate::storage_backend::StorageBackend;

#[derive(Clone)]
pub struct LocalFsBackend {
    base_dir: PathBuf,
}

impl LocalFsBackend {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.base_dir.join(key)
    }
}

#[async_trait]
impl StorageBackend for LocalFsBackend {
    async fn put(&self, key: &str, value: Bytes) -> FlushResult<()> {
        let path = self.full_path(key);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Atomic write: write to a temp file in the same directory, then rename.
        let parent = path.parent().ok_or_else(|| {
            FlushError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "key resolves to a path with no parent directory",
            ))
        })?;

        let temp_path = parent.join(format!(".tmp.{}", uuid::Uuid::now_v7()));
        tokio::fs::write(&temp_path, &value).await?;
        if let Err(e) = tokio::fs::rename(&temp_path, &path).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(e.into());
        }

        Ok(())
    }

    async fn get(&self, key: &str) -> FlushResult<Bytes> {
        let path = self.full_path(key);

        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Bytes::from(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(FlushError::NotFound {
                key: key.to_string(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_range(&self, key: &str, offset: u64, length: u64) -> FlushResult<Bytes> {
        let path = self.full_path(key);

        // Check existence first so we can return NotFound for missing keys,
        // even when length == 0.
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(FlushError::NotFound {
                    key: key.to_string(),
                });
            }
            Err(e) => return Err(e.into()),
        };

        if length == 0 {
            return Ok(Bytes::new());
        }

        let file_size = metadata.len();

        if offset >= file_size {
            return Err(FlushError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "offset {} is at or beyond file size {} for key '{}'",
                    offset, file_size, key
                ),
            )));
        }

        let mut file = tokio::fs::File::open(&path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let available = file_size - offset;
        let to_read = std::cmp::min(length, available) as usize;

        let mut buf = vec![0u8; to_read];
        let mut total_read = 0;
        while total_read < to_read {
            let n = file.read(&mut buf[total_read..]).await?;
            if n == 0 {
                break;
            }
            total_read += n;
        }
        buf.truncate(total_read);

        Ok(Bytes::from(buf))
    }

    async fn delete(&self, key: &str) -> FlushResult<()> {
        let path = self.full_path(key);

        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn conditional_put(&self, key: &str, value: Bytes) -> FlushResult<()> {
        let path = self.full_path(key);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Use O_CREAT | O_EXCL via create_new(true) for atomicity.
        // This must be done in spawn_blocking because std::fs is synchronous.
        let path_clone = path.clone();
        let value_clone = value.clone();
        let key_owned = key.to_string();

        tokio::task::spawn_blocking(move || {
            use std::io::Write;

            let mut file = match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path_clone)
            {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(FlushError::PreconditionFailed {
                        message: format!("key '{}' already exists", key_owned),
                    });
                }
                Err(e) => return Err(e.into()),
            };

            file.write_all(&value_clone)?;
            file.sync_all()?;

            Ok(())
        })
        .await
        .map_err(|e| {
            FlushError::Io(std::io::Error::other(format!(
                "spawn_blocking join error: {}",
                e
            )))
        })?
    }

    async fn list_prefix(&self, prefix: &str) -> FlushResult<Vec<String>> {
        let mut keys = Vec::new();
        collect_keys_recursive(&self.base_dir, &self.base_dir, &mut keys).await?;

        keys.retain(|k| k.starts_with(prefix));
        keys.sort();

        Ok(keys)
    }
}

/// Recursively walks a directory tree and collects all file paths as keys
/// relative to `base_dir`.
async fn collect_keys_recursive(
    base_dir: &std::path::Path,
    current_dir: &std::path::Path,
    keys: &mut Vec<String>,
) -> FlushResult<()> {
    let mut read_dir = match tokio::fs::read_dir(current_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    while let Some(entry) = read_dir.next_entry().await? {
        let file_type = entry.file_type().await?;
        let entry_path = entry.path();

        if file_type.is_dir() {
            Box::pin(collect_keys_recursive(base_dir, &entry_path, keys)).await?;
        } else if file_type.is_file() {
            if let Ok(relative) = entry_path.strip_prefix(base_dir) {
                let key = relative.to_string_lossy().to_string();
                let file_name = entry_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                if !file_name.starts_with(".tmp.") {
                    keys.push(key);
                }
            }
        }
    }

    Ok(())
}
