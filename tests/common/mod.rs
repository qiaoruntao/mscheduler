use std::env;
use std::time::Duration;

use mongodb::{Client, Collection};
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, ResolverConfig};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::task::JoinHandle;

use mscheduler::tasker::consumer::{ConsumerEvent, TaskConsumer, TaskConsumerFunc};
use mscheduler::tasker::task_common::ensure_index;

pub mod test_consume_func;

/// important: clone a collection will not duplicate change stream events
pub async fn init_collection_for_test<T>(collection_name: impl AsRef<str>) -> Collection<T> {
    let connection_str = env::var("MongoStr").expect("need mongodb connection str");
    let client_options = if cfg!(windows) && connection_str.contains("+srv") {
        ClientOptions::parse_with_resolver_config(connection_str, ResolverConfig::quad9()).await.unwrap()
    } else {
        ClientOptions::parse(connection_str).await.unwrap()
    };
    let target_database = client_options.default_database.clone().unwrap();
    // Get a handle to the deployment.
    let client = Client::with_options(client_options).unwrap();
    let database = client.database(target_database.as_str());
    let collection = database.collection::<T>(collection_name.as_ref());
    collection.delete_many(doc! {}, None).await.expect("failed to clean up collection");
    ensure_index(&collection).await;
    collection
}

pub fn spawn_check_handler<T: DeserializeOwned + Send + Unpin + Sync + Clone + 'static, K: Serialize + DeserializeOwned + Send + Unpin + Sync + 'static, Func: TaskConsumerFunc<T, K> + Send, F: Fn(&ConsumerEvent) -> bool + Send + 'static>(task_consumer: TaskConsumer<T, K, Func>, check: F, duration: Duration) -> JoinHandle<Option<ConsumerEvent>> {
    tokio::spawn({
        async move {
            task_consumer.wait_for_event_with_timeout(check, duration).await
        }
    })
}

pub fn spawn_running_consumer_handler<T: DeserializeOwned + Send + Unpin + Sync + Clone + 'static, K: Serialize + DeserializeOwned + Send + Unpin + Sync + 'static, Func: TaskConsumerFunc<T, K> + Send>(task_consumer: TaskConsumer<T, K, Func>) -> JoinHandle<()> {
    tokio::spawn({
        let task_consumer = task_consumer.clone();
        async move {
            task_consumer.start().await;
            // tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    })
}