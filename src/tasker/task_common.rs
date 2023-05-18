use mongodb::{Collection, IndexModel};
use mongodb::bson::doc;
use mongodb::options::IndexOptions;

pub async fn ensure_index<T>(collection: &Collection<T>) {
    let key_index = IndexModel::builder()
        .keys(doc! {"key":1})
        .options({
            let mut options = IndexOptions::default();
            options.name = Some("key".to_string());
            options.unique = Some(true);
            options.background = Some(true);
            options
        })
        .build();
    let _ = collection.create_index(key_index, None).await;
    let worker_id_index = IndexModel::builder()
        .keys(doc! {"key":1,"task_state.worker_states.worker_id": 1})
        .options({
            let mut options = IndexOptions::default();
            options.name = Some("key_worker_id".to_string());
            options.unique = Some(true);
            options.background = Some(true);
            options
        })
        .build();
    let _ = collection.create_index(worker_id_index, None).await;
}