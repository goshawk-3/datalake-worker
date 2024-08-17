use crate::{ChunkId, DataChunk};
use crate::{DatasetId, Error, StorageConf, StorageEngine};
use std::ops::Deref;
use std::path::Path;

use rocksdb_lib::{
    IteratorMode, OptimisticTransactionDB,
    OptimisticTransactionOptions, Transaction,
    WriteOptions,
};
use serde_binary::binary_stream::Endian;

const SIZE_KEY: [u8; 1] = [1u8; 1];

/// Represents a storage engine implementation using RocksDB.
pub struct StorageEngineImpl {
    rocksdb: OptimisticTransactionDB,
    conf: StorageConf,
}

impl StorageEngine for StorageEngineImpl {
    fn from_conf(conf: StorageConf) -> Self {
        let rocksdb =
            OptimisticTransactionDB::open_default(
                conf.path.as_str(),
            )
            .unwrap();
        Self { rocksdb, conf }
    }

    fn find_chunk_id(
        &self,
        dataset_id: DatasetId,
        block_number: u64,
    ) -> Result<Option<ChunkId>, Error> {
        self.try_commit_txn(|txn| {
            let key = to_key(dataset_id, block_number);
            Ok(txn.get(key)?.map(to_chunk_id))
        })
    }

    fn read_chunk_ids(
        &self,
    ) -> Result<Vec<ChunkId>, Error> {
        self.try_commit_txn(|txn| {
            let iter = txn.iterator(IteratorMode::Start);

            iter.map(Result::unwrap)
                .map(|(_, value)| {
                    let chunk_id: ChunkId =
                        value.deref().try_into().unwrap();
                    Ok(chunk_id)
                })
                .collect()
        })
    }

    fn delete_chunk_id(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<(), Error> {
        self.try_commit_txn(|txn| {
            txn.delete(chunk_id)?;

            // Delete any dataset_id+block_num keys that have values of chunk_id
            let iter = txn.iterator(IteratorMode::Start);
            for (key, value) in iter.map(Result::unwrap) {
                if value.deref() == &chunk_id[..] {
                    txn.delete(key)?;
                }
            }

            // TODO: Decrement AllocatedSize value

            Ok(())
        })
    }

    fn persist_chunk(
        &self,
        chunk: DataChunk,
        size: u32,
    ) -> Result<(), Error> {
        self.try_commit_txn(|txn| {
            // Ensure upper limit for persisted chunks is not reached
            let buf = txn
                .get(SIZE_KEY)?
                .expect("SIZE_KEY must always exist");

            let buf = buf.try_into().unwrap();
            let curr_size = u32::from_le_bytes(buf);
            let size_after_this_update = curr_size + size;
            if size_after_this_update
                >= self.conf.max_size_allocated_on_disk
            {
                return Err(Error::MaxSizeAllocated(
                    size_after_this_update,
                ));
            }

            txn.put(
                SIZE_KEY,
                size_after_this_update.to_be_bytes(),
            )?;

            // Persist Key (Database_ID, Block_num) to Chunk_ID
            for block_num in chunk.block_range.clone() {
                let key =
                    to_key(chunk.dataset_id, block_num);
                txn.put(key, chunk.id)?;
            }

            // TODO: Sizeu32

            // Persist KEY - VALUE
            // Chunk_ID -> SizeU32 + Chunk Bytes
            let id = chunk.id;
            let chunk_bytes =
                serde_binary::encode(&chunk, Endian::Big)
                    .expect("should encode");

            // Chunk is now in form of chunk_bytes, drop the chunk struct to free memory
            drop(chunk);

            txn.put(id, chunk_bytes)?;

            Ok(())
        })
    }

    fn chunk_path(&self, _chunk_id: &ChunkId) -> &Path {
        // implementation of chunk_path method
        todo!()
    }
}

impl StorageEngineImpl {
    fn try_commit_txn<T, R>(
        &self,
        func: T,
    ) -> Result<R, Error>
    where
        T: FnOnce(
            &Transaction<'_, OptimisticTransactionDB>,
        ) -> Result<R, Error>,
    {
        // Create a new OptimisticTransaction transaction
        let write_options = WriteOptions::default();
        let tx_options =
            OptimisticTransactionOptions::default();
        let txn = self
            .rocksdb
            .transaction_opt(&write_options, &tx_options);

        let res = func(&txn)?;

        txn.commit()?;
        Ok(res)
    }
}

fn to_chunk_id(v: Vec<u8>) -> ChunkId {
    v.try_into().expect("should be valid id")
}

/// Converts a dataset_id and block_number to a key
fn to_key(
    dataset_id: DatasetId,
    block_number: u64,
) -> [u8; 40] {
    let buf = block_number.to_be_bytes();
    [&dataset_id[..], &buf].concat().try_into().unwrap()
}
