use crate::{
    ChunkId, DataChunk, DataChunkRef, StorageEngine,
};
use aws_config::BehaviorVersion;
use serde_binary::binary_stream::Endian;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{
    OwnedSemaphorePermit, RwLock, Semaphore,
};
use tokio::task::JoinHandle;

type Cache = Arc<RwLock<HashMap<ChunkId, Arc<Semaphore>>>>;

pub struct DataChunkRefImpl<T: StorageEngine> {
    _permit: OwnedSemaphorePermit,
    chunk_id: ChunkId,
    db: Arc<RwLock<T>>,
}

impl<T: StorageEngine> DataChunkRefImpl<T> {
    pub fn new(
        permit: OwnedSemaphorePermit,
        chunk_id: ChunkId,
        db: Arc<RwLock<T>>,
    ) -> Self {
        Self {
            _permit: permit,
            chunk_id,
            db,
        }
    }
}

impl<T: StorageEngine> DataChunkRef
    for DataChunkRefImpl<T>
{
    fn path(&self) -> PathBuf {
        self.db
            .try_read()
            .unwrap()
            .chunk_path(&self.chunk_id)
            .to_path_buf()
    }
}

pub struct DataManagerImpl<T: StorageEngine> {
    /// Cache holds a list of ids of all persisted chunks
    ///
    /// It also maps a lock permit to chunk_id
    cache: Cache,

    /// DB represents the persistence layer
    db: Arc<RwLock<T>>,
}

impl<T: StorageEngine> DataManagerImpl<T> {
    /// Spawns a task to download and persist chunks
    pub fn spawn_download_chunk(
        &self,
        s3_bucket: String,
        s3_key: String,
    ) -> JoinHandle<()> {
        let cache = self.cache.clone();
        let db = self.db.clone();

        let s3_downloader = Self::download_chunk(
            db, cache, s3_bucket, s3_key,
        );
        tokio::spawn(s3_downloader)
    }

    /// Lists all persisted chunks from the Cache
    pub async fn list_chunks(&self) -> Vec<ChunkId> {
        self.cache
            .read()
            .await
            .iter()
            .map(|(chunk_id, _)| *chunk_id)
            .collect()
    }

    /// Returns DataChunkRefImpl, if chunk is found
    /// DataChunkRefImpl
    pub async fn find_chunk(
        &self,
        dataset_id: [u8; 32],
        block_number: u64,
    ) -> Option<impl DataChunkRef> {
        let chunk_id = self
            .db
            .read()
            .await
            .find_chunk_id(dataset_id, block_number)
            .ok()??;

        if let Some(semaphore) =
            self.cache.read().await.get(&chunk_id).cloned()
        {
            return semaphore.try_acquire_owned().ok().map(
                |permit| {
                    DataChunkRefImpl::new(
                        permit,
                        chunk_id,
                        self.db.clone(),
                    )
                },
            );
        }
        None
    }

    /// Spawns a task for chunk deletion
    pub async fn spawn_delete_chunk(
        &self,
        chunk_id: ChunkId,
    ) -> Option<JoinHandle<()>> {
        let semaphore = self
            .cache
            .read()
            .await
            .get(&chunk_id)
            .cloned()?;

        let cache = self.cache.clone();
        let db = self.db.clone();

        let handle = tokio::spawn(async move {
            // If someone has already acquired a DataChunkRefImpl of this chunk
            // then we need to wait here until it's dropped
            let permit = semaphore.acquire().await.unwrap();

            if db
                .read()
                .await
                .delete_chunk_id(&chunk_id)
                .is_ok()
            {
                let mut cache = cache.write().await;
                cache.remove(&chunk_id);
            }

            drop(permit);
        });

        Some(handle)
    }
}

impl<T: StorageEngine> DataManagerImpl<T> {
    pub fn new(storage: T) -> Self {
        // Reload the Cache from the persisted data
        let mut cache = HashMap::new();
        storage
            .read_chunk_ids()
            .expect("readable chunk ids")
            .iter()
            .for_each(|chunk_id| {
                cache.insert(
                    *chunk_id,
                    Arc::new(Semaphore::new(1)),
                );
            });

        Self {
            cache: Arc::new(RwLock::new(cache)),
            db: Arc::new(RwLock::new(storage)),
        }
    }

    /// Implements s3 downloader
    async fn download_chunk(
        db: Arc<RwLock<T>>,
        cache: Cache,
        s3_bucket: String,
        s3_key: String,
    ) {
        let config = aws_config::load_defaults(
            BehaviorVersion::latest(),
        )
        .await;
        let s3_client = aws_sdk_s3::Client::new(&config);

        let mut stream = s3_client
            .get_object()
            .bucket(s3_bucket.clone())
            .key(s3_key.clone())
            .send()
            .await
            .unwrap()
            .body
            .into_async_read();

        let mut vec = Vec::new();
        let chunk_size = vec.len() as u32;

        tokio::io::copy(&mut stream, &mut vec)
            .await
            .expect("valid data blob");

        let chunk: DataChunk =
            serde_binary::from_vec(vec, Endian::Big)
                .expect("valid chunk");

        // Check if the chunk already exists in the cache
        let exists = {
            let mut cache = cache.write().await;
            if !cache.contains_key(&chunk.id) {
                cache.insert(
                    chunk.id,
                    Arc::new(Semaphore::new(1)),
                );
                false
            } else {
                true
            }    
        };

        if !exists {
            // Due to the usage RocksDB::OptimisticTransaction it is safe here to use read lock
            // Any write conflict will be resolved by the RocksDB::commit itself
            if db
                .read()
                .await
                .persist_chunk(&chunk, chunk_size)
                .is_err()
            {
                // chunk could not be persisted. Rollback the cache
                cache.write().await.remove(&chunk.id);
            }
        }
    }
}
