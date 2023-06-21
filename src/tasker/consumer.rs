use std::collections::HashMap;
use std::f32::consts::E;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration};

use async_trait::async_trait;
use futures::{StreamExt};
use mongodb::bson::{bson, DateTime, doc, Document, from_document, to_document};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ChangeStreamEvent;
use mongodb::Collection;
use mongodb::options::{ChangeStreamOptions, FindOneAndUpdateOptions, FullDocumentType, ReturnDocument};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::time::delay_queue::Expired;
use tokio_util::time::DelayQueue;
use tracing::{error, warn};

use crate::tasker::error::{MResult, MSchedulerError};
use crate::tasker::error::MSchedulerError::MongoDbError;
use crate::tasker::task::Task;

#[async_trait]
pub trait TaskConsumerFunc<T: Send, K: Send>: Send + Sync + 'static {
    async fn consumer(&self, params: Option<T>) -> MResult<K>;
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


pub struct TaskState<T: Send, K: Send, Func: TaskConsumerFunc<T, K>> {
    arc: Arc<Func>,
    params: Option<T>,
    handler: JoinHandle<MResult<K>>,
}

impl<T: Send + Clone + 'static, K: Send + 'static, Func: TaskConsumerFunc<T, K>> TaskState<T, K, Func> {
    pub fn run(arc: Arc<Func>, params: Option<T>, sender: Sender<String>, key: String) -> TaskState<T, K, Func> {
        let handler = tokio::spawn({
            let arc = arc.clone();
            let params = params.clone();
            async move {
                let result = arc.consumer(params).await;
                sender.send(key).await;
                result
            }
        });
        TaskState {
            arc,
            params,
            handler,
        }
    }
}

pub struct TaskConsumer<T: Send, K: Send, Func: TaskConsumerFunc<T, K>> {
    marker: PhantomData<Task<T, K>>,
    collection: Collection<Task<T, K>>,
    func: Arc<Func>,
    config: TaskConsumerConfig,
    queue: DelayQueue<String>,
    task_map: HashMap<String, TaskState<T, K, Func>>,
    sender: Sender<String>,
    receiver: Receiver<String>,
}

#[derive(Deserialize)]
struct NextDoc {
    pub key: String,
    pub start_time: DateTime,
}

impl<T: DeserializeOwned + Send + Unpin + Sync + Clone + 'static, K: Serialize + DeserializeOwned + Send + Unpin + Sync + 'static, Func: TaskConsumerFunc<T, K> + Send> TaskConsumer<T, K, Func> {
    pub async fn create(collection: Collection<Task<T, K>>, func: Func, config: TaskConsumerConfig) -> MResult<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(10);
        let consumer = TaskConsumer {
            marker: Default::default(),
            collection,
            func: Arc::new(func),
            config,
            queue: Default::default(),
            task_map: Default::default(),
            sender,
            receiver,
        };
        Ok(consumer)
    }

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
            Some(expired)=futures::future::poll_fn(|cx| self.queue.poll_expired(cx))=>{
                self.try_occupy_task(expired).await
            }
            Some(key)=self.receiver.recv()=>{
                self.post_running(key).await;
            }
        }
    }

    async fn post_running(&mut self, key: String) {
        // 1. check task state
        let task_state = match self.task_map.remove(&key) {
            None => {
                return;
            }
            Some(v) => { v }
        };
        // 2. update its state based on running result
        if !task_state.handler.is_finished() {
            warn!("task state is not finished during post running, key={}",key);
        }
        match task_state.handler.await.unwrap() {
            Ok(returns) => {
                self.mark_task_success(key, returns).await;
            }
            Err(e) => {
                self.mark_task_failed(key, e).await;
            }
        }
        // 3. notify outside components
    }

    pub fn is_task_running(&self, key: impl AsRef<str>) -> bool {
        self.task_map.contains_key(key.as_ref())
    }

    // update worker state to success if not already success
    async fn mark_task_success(&self, key: String, returns: K) -> MResult<()> {
        let query = doc! {
            "key":&key,
            "task_state.worker_states.worker_id": &self.config.worker_id
        };
        let update = doc! {
            "$set": {
                "task_state.worker_states.$.success_time": "$$NOW",
                "task_state.worker_states.$.returns": to_document(&returns).expect("failed to serialize returns"),
            }
        };
        match self.collection.update_one(query, update, None).await {
            Ok(v) => {
                if v.modified_count == 0 {
                    Err(MSchedulerError::NoTaskMatched)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                Err(MongoDbError(e.into()))
            }
        }
    }
    async fn mark_task_failed(&self, key: String, e: MSchedulerError) {}

    async fn try_occupy_task(&mut self, expired: Expired<String>) {
        let deadline = expired.deadline();
        let key = expired.get_ref();
        if deadline + Duration::from_secs(100) < Instant::now() {
            warn!("task key {} expired long ago", key);
            return;
        }
        // otherwise we try to occupy this task
        match self.occupy_task(key).await {
            Ok(task) => {
                // save task state and run it
                let params = task.params.clone();
                let arc = self.func.clone();
                let task_state = TaskState::run(arc, params, self.sender.clone(), task.key.clone());
                self.task_map.insert(task.key.clone(), task_state);
            }
            Err(MSchedulerError::MongoDbError(e)) => {
                error!("failed to occupy task error={}",e);
            }
            Err(_) => {
                // ignore normal errors
            }
        }
    }

    async fn occupy_task(&mut self, key: impl AsRef<str>) -> MResult<Task<T, K>> {
        let worker_id = self.config.worker_id.as_str();
        let filter = doc! {
                "$and":[
                // check for certain key
                doc!{
                    "key":key.as_ref()
                },
                // check worker version
                doc! {
                    "$or": [
                        doc! { "task_option.min_worker_version": doc!{ "$exists": false } },
                        doc!{ "task_option.min_worker_version": doc!{ "$lt": 111 } },
                    ]
                },
                // check specific worker
                doc!{
                    "$or": [
                        doc!{ "task_option.specific_worker_ids": doc!{ "$exists": false } },
                        doc!{ "task_option.specific_worker_ids": doc!{ "$size": 0 } },
                        doc!{ "task_option.specific_worker_ids": worker_id },
                    ]
                },
                // check not already occupied by self
                doc!{
                    "$nor": [doc!{
                        "task_state.worker_states": doc!{
                            "$elemMatch": doc!{
                                "worker_id": worker_id,
                                "ping_expire_time": doc!{ "$gt": "$$NOW" }
                            }
                        }
                    }]
                },
                // check worker count
                doc!{
                    "$or": [
                        doc!{ "task_state.worker_states": doc!{ "$size": 0 } },
                        // optimize for only one worker condition
                        doc!{
                            "$expr": doc!{
                                "$lt": [doc!{
                                    "$size": doc!{
                                        "$filter": doc!{
                                            "input": "$task_state.worker_states",
                                            "as": "item",
                                            "cond": doc!{ "$gt": ["$$item.ping_expire_time", "$$NOW"] }
                                        }
                                    }
                                }, "$task_option.concurrent_worker_cnt"]
                            }
                        },
                    ]
                }
            ]
            };
        let update = vec![doc! {
            "$set": doc!{
                "a": "",
                "task_state.worker_states": doc!{
                    "$concatArrays": [doc!{
                        "$filter": doc!{
                            "input": "$task_state.worker_states",
                            "as": "item",
                            "cond": doc!{ "$gt": ["$$item.ping_expire_time", "$$NOW"] }
                        }
                    }, [doc!{
                        "worker_id": worker_id,
                        "unexpected_retry_cnt": 0,
                        "ping_expire_time": "$$NOW",
                    }]]
                }
            }
        }];
        let mut update_options = FindOneAndUpdateOptions::default();
        update_options.sort = Some(doc! {"task_option.priority": -1});
        update_options.return_document = Some(ReturnDocument::After);
        // update_options.projection = Some(doc! {"task_state": 1});
        match self.collection.find_one_and_update(
            filter, update, Some(update_options),
        ).await {
            Ok(Some(task)) => {
                Ok(task)
            }
            Ok(None) => {
                Err(MSchedulerError::NoTaskMatched)
            }
            Err(e) => {
                Err(MSchedulerError::MongoDbError(e.into()))
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
                Err(MSchedulerError::MongoDbError(e.into()))
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