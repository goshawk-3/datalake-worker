use crate::{
    ChunkId, DataChunk, DataChunkRef, DatasetId,
    StorageEngine,
};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use serde_binary::binary_stream::Endian;
use std::collections::{hash_map, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{
    OwnedSemaphorePermit, RwLock, Semaphore,
};
use tokio::task::JoinHandle;

/// Maximum number of concurrent readers allowed for a chunk
const MAX_CONCURRENT_READERS: usize =
    Semaphore::MAX_PERMITS;
const MAX_SIZE_ON_DISK: u64 = 1_000_000_000_000; // 1TB

type CacheWithSem =
    Arc<RwLock<HashMap<ChunkId, Arc<Semaphore>>>>;

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

#[async_trait]
impl<T: StorageEngine> DataChunkRef
    for DataChunkRefImpl<T>
{
    async fn path(&self) -> PathBuf {
        self.db
            .read()
            .await
            .chunk_path(&self.chunk_id)
            .to_path_buf()
    }
}

pub struct DataManagerImpl<T: StorageEngine> {
    /// Cache holds a list of ids of all persisted chunks
    ///
    /// It also maps a lock permit to chunk_id
    cache: CacheWithSem,

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
            // Wait for all ongoing readers to drop DataChunkRefImpl (permit).
            // Only if all permits are released, the chunk can be deleted
            if let Ok(permit) = semaphore
                .acquire_many(MAX_CONCURRENT_READERS as u32)
                .await
            {
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
            } else {
                println!("Failed to acquire semaphore for chunk deletion");
            }
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
                    Arc::new(Semaphore::new(
                        MAX_CONCURRENT_READERS,
                    )),
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
        cache: CacheWithSem,
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

        tokio::io::copy(&mut stream, &mut vec)
            .await
            .expect("valid data blob");

        let chunk_size = vec.len();
        let chunk: DataChunk =
            serde_binary::from_vec(vec, Endian::Big)
                .expect("valid chunk");
        let id = chunk.id;

        // Check if the chunk already exists, if not we acquire a permit here
        let chunk_ref = {
            let mut cache = cache.write().await;
            if let hash_map::Entry::Vacant(e) =
                cache.entry(chunk.id)
            {
                // A fast (a bit inaccurate) way to check if the storage limit is reached
                if db
                    .read()
                    .await
                    .get_total_allocated_size()
                    + chunk_size as u64
                    > MAX_SIZE_ON_DISK
                {
                    println!("Storage limit reached");
                    return;
                }
                let sem = Arc::new(Semaphore::new(
                    MAX_CONCURRENT_READERS,
                ));

                e.insert(sem.clone());

                sem.try_acquire_owned().ok().map(|permit| {
                    DataChunkRefImpl::new(
                        permit,
                        id,
                        db.clone(),
                    )
                })
            } else {
                None
            }
        };

        if let Some(_chunk_ref) = chunk_ref {
            // Due to the usage RocksDB::OptimisticTransaction it is safe here to use read lock
            // Any write conflict will be detected by the RocksDB::commit itself.
            // However, we don't expect any lock-per-key contention here so commit errors due to
            // conflicts are not expected.
            if let Err(err) =
                db.read().await.persist_chunk(chunk)
            {
                println!(
                    "Failed to persist chunk: {:?}",
                    err
                );
                // chunk could not be persisted.
                // It could be due to IO error.
                // Rollback it from the cache to keep the cache consistent
                cache.write().await.remove(&id);
            }
        }
    }
}
