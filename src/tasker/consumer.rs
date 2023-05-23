use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use mongodb::bson::{DateTime, doc, Document, from_document};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ChangeStreamEvent;
use mongodb::Collection;
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio_util::time::DelayQueue;
use tracing::error;

use crate::tasker::error::{MResult, MSchedulerError};
use crate::tasker::task::Task;

#[async_trait]
pub trait TaskConsumerFunc<T, K> {
    async fn consumer(&self, params: T) -> MResult<K>;
}

#[derive(Deserialize)]
pub struct TaskConsumerConfig {
    // specific this worker's version, used to choose which task to run
    worker_version: u32,
    // specific this worker's id, used to remote control worker behavior, also can be used to choose which task to run
    worker_id: String,
    // whether this worker should continue to try to accept tasks
    allow_consume: bool,
}

pub struct TaskConsumer<T, K, Func: TaskConsumerFunc<T, K>> {
    marker: PhantomData<Task<T, K>>,
    collection: Collection<Task<T, K>>,
    func: Func,
    queue: DelayQueue<String>,
}

#[derive(Deserialize)]
struct NextDoc {
    pub key: String,
    pub start_time: DateTime,
}

impl<T: DeserializeOwned + Send + Unpin + Sync, K: DeserializeOwned + Send + Unpin + Sync, Func: TaskConsumerFunc<T, K>> TaskConsumer<T, K, Func> {
    pub async fn create(collection: Collection<Task<T, K>>, func: Func) -> MResult<Self> {
        let consumer = TaskConsumer {
            marker: Default::default(),
            collection,
            func,
            queue: Default::default(),
        };
        Ok(consumer)
    }
    pub async fn update_config(&self, config: TaskConsumerConfig) {}

    pub fn add2queue(&mut self, key: String, run_time: DateTime) {
        let diff = run_time.timestamp_millis() - DateTime::now().timestamp_millis();
        dbg!(&key, &run_time, &diff);
        if diff <= 0 {
            self.queue.insert(key, Duration::ZERO);
        } else {
            // diff max at about 2 years, we limit it to 1000 seconds
            self.queue.insert(key, Duration::from_millis(diff.min(1_000_000) as u64));
        }
    }

    pub async fn start(&mut self) {
        // 1. TODO: fetch worker config
        let config = TaskConsumerConfig {
            worker_version: 1,
            worker_id: "aaaa".to_string(),
            allow_consume: true,
        };
        // 2. start change stream
        let mut change_stream = self.gen_change_stream(&config).await.unwrap();
        // init next_run_time
        let filter = Self::gen_pipeline(&config);
        let mut cursor = self.collection.aggregate([filter], None).await.unwrap();
        if let Some(Ok(task)) = cursor.next().await {
            let task = from_document::<Task<i32, i32>>(task).unwrap();
            self.add2queue(task.key, task.task_state.start_time);
        }
        // 3. wait to consume task at next_run_time
        tokio::select! {
            Some(result) = change_stream.next()=>{
                match result {
                    Ok(change_event) => {
                        match change_event.full_document {
                            None => {
                                return;
                            }
                            Some(doc) => {
                                self.add2queue(doc.key, doc.start_time)
                            }
                        }
                    }
                    Err(e) => {
                        error!("{}",e);
                        return;
                    }
                }
            }
            Some(e)=futures::future::poll_fn(|cx| self.queue.poll_expired(cx))=>{
                dbg!(&e);
            }
        }
    }

    async fn gen_change_stream(&mut self, config: &TaskConsumerConfig) -> MResult<ChangeStream<ChangeStreamEvent<NextDoc>>> {
        let pipeline = [
            // only consider the task we can run
            Self::gen_pipeline(&config),
            doc! {
                "$project":{
                    // _id cannot get filtered, will get error if filtered
                    "operationType":1_i32,
                    // mongodb-rust says ns field should not get filtered
                    "ns":1_i32,
                    "fullDocument.key":"$fullDocument.key",
                    "fullDocument.start_time":"$fullDocument.task_state.start_time",
                }
            }
        ];
        let mut change_stream_options = ChangeStreamOptions::default();
        change_stream_options.full_document = Some(FullDocumentType::UpdateLookup);
        match self.collection.clone_with_type::<NextDoc>().watch(pipeline, Some(change_stream_options)).await {
            Ok(v) => {
                Ok(v)
            }
            Err(e) => {
                Err(MSchedulerError::MongoDbError(e))
            }
        }
    }

    fn gen_pipeline(config: &TaskConsumerConfig) -> Document {
        doc! {
            "$match":{
                "$and":[
                    doc! {
                        "$or":[
                            doc!{ "task_option.min_worker_version": doc!{ "$exists": false } },
                            doc!{ "task_option.min_worker_version": doc!{ "$lt": &config.worker_version } },
                        ]
                    },
                    doc! {
                        "$or":[
                            doc!{ "task_option.specific_worker_ids": doc!{ "$exists": false } },
                            doc!{ "task_option.specific_worker_ids": doc!{ "$size": 0 } },
                            doc!{ "task_option.specific_worker_ids": &config.worker_id },
                        ]
                    },
                    doc! {
                        "$or":[
                            doc!{ "task_state.worker_states": doc!{ "$size": 0 } },
                            doc!{
                                "$expr": doc!{
                                    "$lt": [doc!{
                                        "$size": doc!{
                                            "$filter": doc!{
                                                "input": "$task_state.worker_states",
                                                "as": "item",
                                                "cond": doc!{ "$gt": vec!["$$item.ping_expire_time", "$$NOW"] }
                                            }
                                        }
                                    }, "$task_option.concurrent_worker_cnt"]
                                }
                            },
                        ]
                    },
                ]
            }
        }
    }
}