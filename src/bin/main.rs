use datalake::{
    data_manager::DataManagerImpl,
    rocksdb_storage_engine::StorageEngineImpl,
    StorageEngine,
};

enum Commands {
    List,
    FindWithLock,
    ScheduledDelete,
    Exit,
}

fn prompt() -> requestty::Result<Commands> {
    let answer = requestty::prompt_one(
        requestty::Question::select("command")
            .message("Datalake commands")
            .choice("List persisted chunks")
            .choice("Find chunk")
            .choice("Schedule Deletion")
            .choice("Exit"),
    )?;

    match answer.as_list_item().unwrap().index {
        0 => Ok(Commands::List),
        1 => Ok(Commands::FindWithLock),
        2 => Ok(Commands::ScheduledDelete),
        3 => Ok(Commands::Exit),
        _ => unreachable!(),
    }
}

#[tokio::main]
async fn main() {
     // TODO: Read conf from TOML file

    // Initialize the data manager
    let storage =
        StorageEngineImpl::from_conf(Default::default());

    let data_manager: DataManagerImpl<StorageEngineImpl> = DataManagerImpl::new(storage);

    // Assigned subset of data chunks in S3
    let vec_buckets_and_keys = [
        ("bucket1".to_string(), "key1".to_string()),
        ("bucket2".to_string(), "key2".to_string()),
        ("bucket3".to_string(), "key3".to_string()),
    ];

    // Spawn tasks to concurrently download and persist chunks
    vec_buckets_and_keys.iter().for_each(
        |(bucket, key)| {
            data_manager.spawn_download_chunk(
                bucket.clone(),
                key.clone(),
            );
        },
    );

    // Serve queries for the downloaded data chunks
    loop {
        match prompt().unwrap() {
            Commands::List => {
                println!("List of persisted chunks");

                data_manager
                    .list_chunks()
                    .await
                    .iter()
                    .for_each(|chunk_id| {
                        println!(
                            "chunk id: {}",
                            hex::encode(chunk_id)
                        )
                    });
            }
            Commands::FindWithLock => {
                // TODO: Read input from user
                let dataset_id = [0u8; 32];
                let block_number = 0;

                let chunk_ref = data_manager
                    .find_chunk(dataset_id, block_number)
                    .await;

                if let Some(chunk_ref) = chunk_ref {
                    println!(
                        "Chunk found: {:?}",
                        chunk_ref.path()
                    );
                } else {
                    println!("Chunk not found");
                }
            }
            Commands::ScheduledDelete => {
                // TODO: Read input from user
                // schedule a chunk for deletion
                let chunk_id = [0u8; 32];
                data_manager
                    .spawn_delete_chunk(chunk_id)
                    .await;
            }
            Commands::Exit => {
                // Implement a graceful shutdown
                todo!()
            }
        }
    }
}
