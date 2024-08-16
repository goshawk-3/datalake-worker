pub mod data_manager;
pub mod rocksdb_storage_engine;

use async_trait::async_trait;
use serde_binary::Encode;
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];

pub const EMPTY_CHUNK_ID: ChunkId = [0u8; 32];
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DataChunk {
    id: ChunkId,
    /// Dataset (blockchain) id
    dataset_id: DatasetId,
    /// Block range this chunk is responsible for
    block_range: Range<u64>,
    /// Data chunk files.
    /// A mapping between file names and HTTP URLs to
    /// download files from
    files: HashMap<String, String>,
}

impl Encode for DataChunk {
    fn encode(
        &self,
        _serializer: &mut serde_binary::Serializer<'_>,
    ) -> Result<(), serde_binary::Error> {
        // TODO: Implement
        Ok(())
    }
}

// Data chunk must remain available and untouched till this reference is not dropped
#[async_trait]
pub trait DataChunkRef: Send + Sync {
    // Data chunk directory
    async fn path(&self) -> PathBuf;
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Internal error {0}")]
    InternalErr(rocksdb_lib::Error),
    #[error("Allocated on-disk has reached the maximum limit allowed")]
    MaxSizeAllocated(u32),
}

impl From<rocksdb_lib::Error> for Error {
    fn from(e: rocksdb_lib::Error) -> Self {
        Self::InternalErr(e)
    }
}

#[derive(Debug)]
pub struct StorageConf {
    /// Database file path
    path: String,

    /// Database size on-disk limit
    max_size_allocated_on_disk: u32,
}

impl Default for StorageConf {
    fn default() -> Self {
        Self {
            path: "/tmp/rocksdb".to_string(),
            max_size_allocated_on_disk: 1_000_000,
        }
    }
}

/// Implement StorageEngine to achieve backend-agnostic storage engine
/// Defines a set of functionalities any supported storage
/// engine must implement
pub trait StorageEngine: Send + Sync + 'static {
    fn from_conf(conf: StorageConf) -> Self;

    fn find_chunk_id(
        &self,
        dataset_id: DatasetId,
        block_number: u64,
    ) -> Result<Option<ChunkId>, Error>;

    fn read_chunk_ids(&self)
        -> Result<Vec<ChunkId>, Error>;

    fn delete_chunk_id(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<(), Error>;

    fn persist_chunk(
        &self,
        chunk: DataChunk,
        size: u32,
    ) -> Result<(), Error>;

    fn chunk_path(&self, _chunk_id: &ChunkId) -> &Path;
}

#[cfg(test)]
mod tests {}
