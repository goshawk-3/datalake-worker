# datalake-worker
Data lake implementation integrated with S3

# Supported features

- Async download a chunk from S3
- Persist on-disk in a lock-less manner
- List all persisted chunks by ID from a cache
- Find and lock a chunk - Once locked, chunk cannot be deleted
- Scheduled deletion - Scheduled for deletion, a chunk will be removed once it is no longer in use.

- Backend-agnostic datamanager. The RocksDB backend can be substituted with any in-process NoSQL or SQL storage engine.


# Design 

![image info](./design.png)
