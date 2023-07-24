use mongodb::{Client, Collection};
use mongodb::options::{ClientOptions, ResolverConfig};

pub async fn get_collection<T>(connection_str: impl AsRef<str>, collection_name: impl AsRef<str>) -> Collection<T> {
    let client_options = if cfg!(windows) && connection_str.as_ref().contains("+srv") {
        ClientOptions::parse_with_resolver_config(connection_str, ResolverConfig::quad9()).await.unwrap()
    } else {
        ClientOptions::parse(connection_str).await.unwrap()
    };
    let target_database = client_options.default_database.clone().unwrap();
    // Get a handle to the deployment.
    let client = Client::with_options(client_options).unwrap();
    let database = client.database(target_database.as_str());
    let collection = database.collection::<T>(collection_name.as_ref());
    collection
}