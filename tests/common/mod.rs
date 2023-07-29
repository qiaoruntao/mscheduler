use std::env;

use mongodb::{Client, Collection};
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, ResolverConfig};
use mscheduler::tasker::task_common::ensure_index;

pub async fn get_collection<T>(collection_name: impl AsRef<str>) -> Collection<T> {
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