use flushdb_types::{FlushError, FlushResult, StorageBackend};

use super::types::{
    Manifest, ManifestConfig, ManifestId, ManifestUpdate, ManifestUpdateTrigger, manifest_path,
    manifest_prefix,
};

const MAX_CAS_RETRIES: usize = 5;

pub struct ManifestManager<B: StorageBackend> {
    backend: B,
    current: Manifest,
    config: ManifestConfig,
    namespace: String,
    pub writer_epoch: u64,
    pub compactor_epoch: u64,
}

impl<B: StorageBackend> ManifestManager<B> {
    pub fn new(backend: B, namespace: String, config: ManifestConfig) -> Self {
        Self {
            current: Manifest::new_empty(&namespace),
            backend,
            config,
            namespace,
            writer_epoch: 0,
            compactor_epoch: 0,
        }
    }

    pub async fn load_latest(&mut self) -> FlushResult<&Manifest> {
        let prefix = manifest_prefix(&self.config.base_path, &self.namespace);
        let entries = self.backend.list_prefix(&prefix).await?;

        if entries.is_empty() {
            // Fresh namespace — store the initial empty manifest
            let manifest = Manifest::new_empty(&self.namespace);
            let path = manifest_path(
                &self.config.base_path,
                &self.namespace,
                &ManifestId::ZERO,
            );
            let data = manifest.serialize()?;
            // Use conditional_put for the initial write
            match self.backend.conditional_put(&path, data).await {
                Ok(()) => {}
                Err(FlushError::PreconditionFailed { .. }) => {
                    // Another writer beat us — load what they wrote
                    let stored = self.backend.get(&path).await?;
                    self.current = Manifest::deserialize(&stored)?;
                    return Ok(&self.current);
                }
                Err(e) => return Err(e),
            }
            self.current = manifest;
            return Ok(&self.current);
        }

        // Pick the highest ManifestId (list_prefix returns lexicographically sorted)
        let last_path = entries.last().unwrap();
        let filename = last_path
            .rsplit('/')
            .next()
            .unwrap_or(last_path);
        let highest_id = ManifestId::from_path_string(filename)?;

        let path = manifest_path(&self.config.base_path, &self.namespace, &highest_id);
        let data = self.backend.get(&path).await?;
        let manifest = Manifest::deserialize(&data)?;

        if manifest.format_version != 1 {
            return Err(FlushError::CorruptedData {
                message: format!(
                    "unsupported manifest format_version: {}",
                    manifest.format_version
                ),
            });
        }

        self.current = manifest;
        Ok(&self.current)
    }

    pub async fn load_specific(&mut self, id: ManifestId) -> FlushResult<&Manifest> {
        let path = manifest_path(&self.config.base_path, &self.namespace, &id);
        let data = self.backend.get(&path).await?;
        let manifest = Manifest::deserialize(&data)?;
        self.current = manifest;
        Ok(&self.current)
    }

    pub fn current(&self) -> &Manifest {
        &self.current
    }

    pub async fn update(&mut self, update: ManifestUpdate) -> FlushResult<&Manifest> {
        // Epoch validation
        self.validate_epoch(&update)?;

        // Validate inputs before CAS loop
        self.validate_update(&update)?;

        for attempt in 0..MAX_CAS_RETRIES {
            let new_manifest = update.apply(&self.current)?;

            // Check if should be a snapshot
            let should_snap = self.should_snapshot_manifest(&new_manifest);
            let mut to_write = new_manifest.clone();
            if should_snap {
                to_write.is_snapshot = true;
            }

            let serialized = to_write.serialize()?;
            let path = manifest_path(
                &self.config.base_path,
                &self.namespace,
                &to_write.manifest_id,
            );

            match self.backend.conditional_put(&path, serialized).await {
                Ok(()) => {
                    self.current = to_write;
                    return Ok(&self.current);
                }
                Err(FlushError::PreconditionFailed { .. }) => {
                    if attempt + 1 >= MAX_CAS_RETRIES {
                        return Err(FlushError::ResourceExhausted {
                            resource: "manifest CAS".into(),
                            message: format!(
                                "exceeded {MAX_CAS_RETRIES} CAS retries for manifest update"
                            ),
                        });
                    }
                    // Refresh and re-validate
                    self.refresh().await?;
                    self.validate_epoch(&update)?;
                    // Check inputs still exist
                    for (level, id) in &update.remove_sstables {
                        let ssts = self.current.sstables_at_level(*level);
                        if !ssts.iter().any(|m| m.id == *id) {
                            return Err(FlushError::InvalidArgument {
                                message: format!(
                                    "SSTable {id} at {level} was consumed by concurrent operation"
                                ),
                            });
                        }
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        unreachable!("loop should have returned or errored")
    }

    pub async fn acquire_writer_epoch(&mut self) -> FlushResult<u64> {
        self.refresh().await?;
        let new_epoch = self.current.writer_epoch + 1;

        let update = ManifestUpdate {
            trigger: ManifestUpdateTrigger::Flush,
            add_sstables: vec![],
            remove_sstables: vec![],
            new_last_flushed_sequence: None,
            writer_epoch: new_epoch,
            compactor_epoch: self.current.compactor_epoch,
        };

        // Apply directly — we skip epoch check for epoch acquisition
        let new_manifest = update.apply(&self.current)?;
        let serialized = new_manifest.serialize()?;
        let path = manifest_path(
            &self.config.base_path,
            &self.namespace,
            &new_manifest.manifest_id,
        );

        self.backend.conditional_put(&path, serialized).await?;
        self.current = new_manifest;
        self.writer_epoch = new_epoch;
        Ok(new_epoch)
    }

    pub async fn acquire_compactor_epoch(&mut self) -> FlushResult<u64> {
        self.refresh().await?;
        let new_epoch = self.current.compactor_epoch + 1;

        let update = ManifestUpdate {
            trigger: ManifestUpdateTrigger::Compaction,
            add_sstables: vec![],
            remove_sstables: vec![],
            new_last_flushed_sequence: None,
            writer_epoch: self.current.writer_epoch,
            compactor_epoch: new_epoch,
        };

        let new_manifest = update.apply(&self.current)?;
        let serialized = new_manifest.serialize()?;
        let path = manifest_path(
            &self.config.base_path,
            &self.namespace,
            &new_manifest.manifest_id,
        );

        self.backend.conditional_put(&path, serialized).await?;
        self.current = new_manifest;
        self.compactor_epoch = new_epoch;
        Ok(new_epoch)
    }

    pub fn check_writer_epoch(&self) -> FlushResult<()> {
        if self.current.writer_epoch > self.writer_epoch {
            return Err(FlushError::EpochFenced {
                expected: self.writer_epoch,
                actual: self.current.writer_epoch,
            });
        }
        Ok(())
    }

    pub fn check_compactor_epoch(&self) -> FlushResult<()> {
        if self.current.compactor_epoch > self.compactor_epoch {
            return Err(FlushError::EpochFenced {
                expected: self.compactor_epoch,
                actual: self.current.compactor_epoch,
            });
        }
        Ok(())
    }

    pub async fn refresh(&mut self) -> FlushResult<&Manifest> {
        self.load_latest().await
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    // --- Snapshot support (Task 3) ---

    fn should_snapshot_manifest(&self, manifest: &Manifest) -> bool {
        let id = manifest.manifest_id.as_u64();
        if id > 0 && id.is_multiple_of(self.config.snapshot_interval) {
            return true;
        }
        if let Ok(data) = manifest.serialize() {
            if data.len() > self.config.max_manifest_size {
                return true;
            }
        }
        false
    }

    pub async fn prune_old_manifests(&self) -> FlushResult<usize> {
        let prefix = manifest_prefix(&self.config.base_path, &self.namespace);
        let entries = self.backend.list_prefix(&prefix).await?;

        if entries.len() <= 2 {
            return Ok(0);
        }

        // Find all snapshot manifest IDs by loading each
        let mut snapshot_ids: Vec<ManifestId> = Vec::new();
        for entry_path in &entries {
            let filename = entry_path.rsplit('/').next().unwrap_or(entry_path);
            if let Ok(id) = ManifestId::from_path_string(filename) {
                let path = manifest_path(&self.config.base_path, &self.namespace, &id);
                if let Ok(data) = self.backend.get(&path).await {
                    if let Ok(manifest) = Manifest::deserialize(&data) {
                        if manifest.is_snapshot {
                            snapshot_ids.push(id);
                        }
                    }
                }
            }
        }

        snapshot_ids.sort();

        if snapshot_ids.len() < 2 {
            return Ok(0);
        }

        // The cutoff is the second-most-recent snapshot
        let cutoff = snapshot_ids[snapshot_ids.len() - 2];
        let current_id = self.current.manifest_id;

        let mut to_delete = Vec::new();
        for entry_path in &entries {
            let filename = entry_path.rsplit('/').next().unwrap_or(entry_path);
            if let Ok(id) = ManifestId::from_path_string(filename) {
                if id < cutoff && id != current_id {
                    to_delete.push(entry_path.clone());
                }
            }
        }

        let mut deleted = 0;
        for batch in to_delete.chunks(self.config.pruning_batch_size) {
            for path in batch {
                self.backend.delete(path).await?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    fn validate_epoch(&self, update: &ManifestUpdate) -> FlushResult<()> {
        match update.trigger {
            ManifestUpdateTrigger::Flush => self.check_writer_epoch(),
            ManifestUpdateTrigger::Compaction => self.check_compactor_epoch(),
            ManifestUpdateTrigger::GC => Ok(()),
        }
    }

    fn validate_update(&self, update: &ManifestUpdate) -> FlushResult<()> {
        // Check removes exist
        for (level, id) in &update.remove_sstables {
            let ssts = self.current.sstables_at_level(*level);
            if !ssts.iter().any(|m| m.id == *id) {
                return Err(FlushError::InvalidArgument {
                    message: format!("SSTable {id} not found at level {level}"),
                });
            }
        }

        // Check adds don't already exist
        for (level, meta) in &update.add_sstables {
            let ssts = self.current.sstables_at_level(*level);
            if ssts.iter().any(|m| m.id == meta.id) {
                return Err(FlushError::InvalidArgument {
                    message: format!("SSTable {} already exists at level {level}", meta.id),
                });
            }
        }

        // Check sequence doesn't decrease
        if let Some(seq) = update.new_last_flushed_sequence {
            if seq < self.current.last_flushed_sequence {
                return Err(FlushError::InvalidArgument {
                    message: format!(
                        "last_flushed_sequence cannot decrease from {} to {}",
                        self.current.last_flushed_sequence, seq
                    ),
                });
            }
        }

        Ok(())
    }
}
