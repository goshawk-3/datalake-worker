use crate::{ChunkId, DataChunk};
use crate::{DatasetId, Error, StorageConf, StorageEngine};
use std::ops::{Deref, Range};
use std::path::Path;

use rocksdb_lib::{
    IteratorMode, OptimisticTransactionDB,
    OptimisticTransactionOptions, Transaction,
    WriteOptions,
};
use serde_binary::binary_stream::Endian;

const CF_CHUNKS: &str = "cf_chunks";

/// Represents a storage engine implementation using RocksDB.
pub struct StorageEngineImpl {
    rocksdb: OptimisticTransactionDB,
    conf: StorageConf,
}

impl StorageEngine for StorageEngineImpl {
    fn from_conf(conf: StorageConf) -> Self {
        // TODO: Create all used column families

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
            let key = to_key(
                dataset_id,
                Range {
                    start: block_number,
                    end: block_number + 1,
                },
            );

            Ok(txn.get(key)?.map(to_chunk_id))
        })
    }

    fn read_chunk_ids(
        &self,
    ) -> Result<Vec<ChunkId>, Error> {
        self.try_commit_txn(|txn| {
            if let Some(chunks_cf) =
                self.rocksdb.cf_handle(CF_CHUNKS)
            {
                let iter = txn.iterator_cf(
                    chunks_cf,
                    IteratorMode::Start,
                );

                iter.map(Result::unwrap)
                    .map(|(_, value)| {
                        let chunk_id: ChunkId = value
                            .deref()
                            .try_into()
                            .unwrap();
                        Ok(chunk_id)
                    })
                    .collect()
            } else {
                Ok(vec![])
            }
        })
    }

    fn delete_chunk_id(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<(), Error> {
        self.try_commit_txn(|txn| {
            let chunks_cf =
                self.rocksdb.cf_handle(CF_CHUNKS).expect(
                    "CF_CHUNKS column family must exist",
                );

            txn.delete_cf(chunks_cf, chunk_id)?;

            // Chunk_id , dataset_id_chunk_id

            // Delete any dataset_id+block_num keys that have values of chunk_id
            let iter = txn.iterator(IteratorMode::Start);
            for (key, value) in iter.map(Result::unwrap) {
                if value.deref() == &chunk_id[..] {
                    txn.delete(key)?;
                }
            }

            Ok(())
        })
    }

    fn persist_chunk(
        &self,
        chunk: DataChunk,
    ) -> Result<(), Error> {
        self.try_commit_txn(|txn| {
            // Persist Key (Database_ID, Block_Range) to Chunk_ID

            let key = to_key(
                chunk.dataset_id,
                chunk.block_range.clone(),
            );
            txn.put(key, chunk.id)?;

            let id = chunk.id;
            let chunk_bytes =
                serde_binary::encode(&chunk, Endian::Big)
                    .expect("should encode");

            // Chunk is now in form of chunk_bytes, drop the chunk struct to free memory
            drop(chunk);

            // Chunk_ID -> Chunk Bytes
            let chunks_cf =
                self.rocksdb.cf_handle(CF_CHUNKS).expect(
                    "CF_CHUNKS column family must exist",
                );

            txn.put_cf(chunks_cf, id, chunk_bytes)?;

            Ok(())
        })
    }

    fn chunk_path(&self, _chunk_id: &ChunkId) -> &Path {
        // implementation of chunk_path method
        todo!()
    }

    /// Returns the actual size of the data stored after compression
    fn get_total_allocated_size(&self) -> u64 {
        // TODO: Use SstFileManager::GetTotalSize().
        // https://github.com/facebook/rocksdb/wiki/Managing-Disk-Space-Utilization#usage
        0
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
    block_number: Range<u64>,
) -> [u8; 40] {
    let buf_start = block_number.start.to_be_bytes();
    let buf_end = block_number.end.to_be_bytes();

    [&dataset_id[..], &buf_start, &buf_end]
        .concat()
        .try_into()
        .unwrap()
}
