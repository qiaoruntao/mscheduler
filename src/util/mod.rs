use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};

pub async fn get_collection<T: Send + Sync>(
    connection_str: impl AsRef<str>,
    collection_name: impl AsRef<str>,
) -> Collection<T> {
    let client_options = ClientOptions::parse(connection_str.as_ref()).await.unwrap();
    let target_database = client_options.default_database.clone().unwrap();
    // Get a handle to the deployment.
    let client = Client::with_options(client_options).unwrap();
    let database = client.database(target_database.as_str());
    let collection = database.collection::<T>(collection_name.as_ref());
    collection
}

pub async fn get_collection_result<T: Send + Sync>(
    connection_str: impl AsRef<str>,
    collection_name: impl AsRef<str>,
) -> Collection<T> {
    let client_options = ClientOptions::parse(connection_str.as_ref()).await.unwrap();
    let target_database = client_options.default_database.clone().unwrap();
    // Get a handle to the deployment.
    let client = Client::with_options(client_options).unwrap();
    let database = client.database(target_database.as_str());
    let collection = database.collection::<T>(collection_name.as_ref());
    collection
}
