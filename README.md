# datalake-worker
Data lake implementation integrated with AWS S3

# Supported features

- Async-Download chunks from AWS S3
- Persist on-disk in a lock-less manner
- List all persisted chunks by ID from a cache
- Find and lock a chunk - Once locked, chunk cannot be deleted until all DataChunkRef are dropped.
- Scheduled deletion - Scheduled for deletion, a chunk will be removed once it is no longer in use.

- Maximum allocated on-disk storage limit
- Backend-agnostic datamanager. The RocksDB backend can be substituted with any in-process NoSQL or SQL storage engine.g
- Simple Prompt UI

# Design 

![image info](./design.png)

# Datasource structure

### Cache - in-memory map of chunks IDs to a Semaphore

| Chunk_ID    | Semaphore |
| -------- | ------- |
| 0x0A0B    |  Instance   |
| 0x0A0C    | Instance    |
| 0x0A0C    | Instance    |

### OnDisk Tables and Indexes

| Chunk_ID    | Encoded Chunk Data |
| -------- | ------- |
| 0x0A0B    |  0x..   |
| 0x0A0C    | 0x...    |
| 0x0A0C    | 0x    |

| DatasetID_BlockRange   | Chunk_ID |
| -------- | ------- |
| 100_0_100    | 0x0A0B   |
| 100_101_120    | 0x0A0C    |
| 100_121_1000    | 0x0C0A    |
 

