use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use mongodb::bson::{Bson, DateTime, doc, Document, from_document, to_bson};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ChangeStreamEvent;
use mongodb::Collection;
use mongodb::options::{ChangeStreamOptions, FindOneAndUpdateOptions, FullDocumentType, ReturnDocument};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::time::delay_queue::Expired;
use tokio_util::time::DelayQueue;
use tracing::{error, trace, warn};
use typed_builder::TypedBuilder;

use crate::tasker::error::{MResult, MSchedulerError};
use crate::tasker::error::MSchedulerError::{ExecutionError, MongoDbError, NoTaskMatched, PanicError, TaskCancelled, UnknownError};
use crate::tasker::task::Task;

#[async_trait]
pub trait TaskConsumerFunc<T: Send, K: Send>: Send + Sync + 'static {
    async fn consumer(&self, params: Option<T>) -> MResult<K>;
}

#[derive(Deserialize, TypedBuilder, Default)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TaskConsumerConfig {
    // specific this worker's version, used to choose which task to run
    #[builder(default = 0)]
    pub worker_version: u32,
    // specific this worker's id, used to remote control worker behavior, also can be used to choose which task to run
    #[builder(default_code = "hostname::get().unwrap_or_default().into_string().ok()", setter(strip_option))]
    pub worker_id: Option<String>,
}

impl TaskConsumerConfig {
    pub fn get_worker_version(&self) -> u32 {
        self.worker_version
    }

    pub fn get_worker_id(&self) -> &str {
        match &self.worker_id {
            None => {
                "default"
            }
            Some(v) => {
                v.as_str()
            }
        }
    }
}


pub struct TaskState<K: Send> {
    handler: JoinHandle<MResult<K>>,
}

impl<K: Send + 'static> TaskState<K> {
    pub fn run<T: Send + Clone + 'static, Func: TaskConsumerFunc<T, K>>(arc: Arc<Func>, params: Option<T>, sender: Sender<String>, key: String) -> TaskState<K> {
        let handler = tokio::spawn({
            let arc = arc.clone();
            let params = params.clone();
            async move {
                let result = match tokio::spawn(async move { arc.consumer(params).await }).catch_unwind().await {
                    Ok(Ok(v)) => { v }
                    Ok(Err(e)) => {
                        if e.is_panic() {
                            Err(PanicError)
                        } else if e.is_cancelled() {
                            Err(TaskCancelled)
                        } else {
                            unreachable!();
                        }
                    }
                    Err(_) => {
                        error!("unexpected error found");
                        Err(UnknownError)
                    }
                };
                if let Err(e) = sender.send(key).await {
                    error!("failed to send task state update message {}",&e);
                }
                result
            }
        });
        TaskState {
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
    task_map: HashMap<String, TaskState<K>>,
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
        trace!("add key {} to wait queue",&key);
        if diff <= 0 {
            self.queue.insert(key, Duration::ZERO);
        } else {
            // diff max at about 2 years, we limit it to 1000 seconds
            self.queue.insert(key, Duration::from_millis(diff.min(1_000_000) as u64));
        }
    }

    pub async fn start(&mut self) {
        // 1. TODO: fetch worker config
        // 2. start change stream
        let mut change_stream = self.gen_change_stream().await.unwrap();
        // init next_run_time
        let filter = Self::gen_pipeline(&self.config);
        let mut cursor = self.collection.aggregate([filter], None).await.unwrap();
        if let Some(Ok(task)) = cursor.next().await {
            let task = from_document::<Task<i32, i32>>(task).unwrap();
            self.add2queue(task.key, task.task_state.start_time);
        }
        trace!("start to wait for change stream, worker_id={}", self.config.get_worker_id());
        // 3. wait to consume task at next_run_time
        loop {
            tokio::select! {
                Some(result) = change_stream.next()=>{
                    match result {
                        Ok(change_event) => {
                            match change_event.full_document {
                                None => {
                                    warn!("full document not provided");
                                    return;
                                }
                                Some(doc) => {
                                    self.add2queue(doc.key, doc.start_time)
                                }
                            }
                        }
                        Err(e) => {
                            error!("change stream failed {}",e);
                            return;
                        }
                    }
                }
                Some(expired)=futures::future::poll_fn(|cx| self.queue.poll_expired(cx))=>{
                    self.try_occupy_task(expired).await
                }
                Some(key)=self.receiver.recv()=>{
                    if let Err(e)=self.post_running(key).await{
                        error!("failed to execute post running {}",&e);
                    }
                }
            }
        }
    }

    pub fn is_task_running(&self, key: impl AsRef<str>) -> bool {
        self.task_map.contains_key(key.as_ref())
    }

    async fn post_running(&mut self, key: String) -> MResult<()> {
        trace!("start to post handling key {} worker_id {}", key, self.config.get_worker_id());
        // 1. check task state
        let task_state = match self.task_map.remove(&key) {
            None => {
                error!("failed to get task state for key {}", &key);
                return Err(NoTaskMatched);
            }
            Some(v) => { v }
        };
        // 2. update its state based on running result
        if !task_state.handler.is_finished() {
            warn!("task state is not finished during post running, key={}",key);
        }
        let handle_result = match task_state.handler.await.unwrap() {
            Ok(returns) => {
                self.mark_task_success(key, returns).await
            }
            Err(e) => {
                self.mark_task_failed(key, e).await
            }
        };
        // TODO: 3. notify outside components
        handle_result
    }

    // update worker state to success if not already success
    async fn mark_task_success(&self, key: String, returns: K) -> MResult<()> {
        let query = doc! {
            "key":&key,
            "task_state.worker_states.worker_id": self.config.get_worker_id()
        };
        let update = doc! {
            "$currentDate":{
                "task_state.worker_states.$.success_time": true,
            },
            "$set": {
                "task_state.worker_states.$.returns": to_bson(&returns).expect("failed to serialize returns"),
            },
            "$unset":{
                "task_state.worker_states.$.fail_time": "",
            }
        };
        match self.collection.update_one(query, update, None).await {
            Ok(v) => {
                if v.modified_count == 0 {
                    error!("failed to set task {} to success", &key);
                    Err(NoTaskMatched)
                } else {
                    trace!("set task {} to success", &key);
                    Ok(())
                }
            }
            Err(e) => {
                error!("failed to set task {} to success, {}", &key, &e);
                Err(MongoDbError(e.into()))
            }
        }
    }
    async fn mark_task_failed(&self, key: String, e: MSchedulerError) -> MResult<()> {
        let update = if let ExecutionError(_) = e {
            doc! {
                "$currentDate":{"task_state.worker_states.$.fail_time": true},
                "$set": {
                    "task_state.worker_states.$.fail_reason": format!("{}",e),
                },
                "$unset":{
                    "task_state.worker_states.$.success_time": Bson::Null,
                    "task_state.worker_states.$.returns": Bson::Null
                }
            }
        } else {
            doc! {
                "$currentDate":{"task_state.worker_states.$.fail_time": true},
                "$set": {
                    "task_state.worker_states.$.fail_reason": format!("{}",e),
                },
                "$unset":{
                    "task_state.worker_states.$.success_time": Bson::Null,
                    "task_state.worker_states.$.returns": Bson::Null
                }
            }
        };
        let query = doc! {
                "key": &key,
                "task_state.worker_states.worker_id": self.config.get_worker_id()
            };
        match self.collection.update_one(query, update, None).await {
            Ok(v) => {
                if v.modified_count == 0 {
                    error!("failed to set task {} to failed", &key);
                    Err(NoTaskMatched)
                } else {
                    trace!("set task {} to fail", &key);
                    Ok(())
                }
            }
            Err(e) => {
                error!("failed to set task {} to fail, {}", &key, &e);
                Err(MongoDbError(e.into()))
            }
        }
    }

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
                trace!("success to occupy task key {}",&key);
                self.task_map.insert(task.key.clone(), task_state);
            }
            Err(MongoDbError(e)) => {
                error!("failed to occupy task error={}",e);
            }
            Err(e) => {
                trace!("failed to occupy task key {}, error {}",&key, &e);
                // ignore normal errors
            }
        }
    }

    async fn occupy_task(&mut self, key: impl AsRef<str>) -> MResult<Task<T, K>> {
        let worker_id = self.config.get_worker_id();
        let filter = doc! {
                "$and":[
                    // check for certain key
                    {
                        "key":key.as_ref()
                    },
                    // check worker version
                     {
                        "$or": [
                            { "task_option.min_worker_version": { "$exists": false } },
                            { "task_option.min_worker_version": { "$lte": &self.config.get_worker_version() } },
                        ]
                    },
                    // check specific worker
                    {
                        "$or": [
                            { "task_option.specific_worker_ids": { "$exists": false } },
                            { "task_option.specific_worker_ids": { "$size": 0 } },
                            { "task_option.specific_worker_ids": worker_id },
                        ]
                    },
                    // check not already occupied by self
                    {
                        "$nor": [{
                            "task_state.worker_states": {
                                "$elemMatch": {
                                    "worker_id": worker_id,
                                    "ping_expire_time": { "$gt": "$$NOW" }
                                }
                            }
                        }]
                    },
                    // check worker count
                    {
                        "$or": [
                            { "task_state.worker_states": { "$size": 0 } },
                            // optimize for only one worker condition
                            {
                                "$expr": {
                                    "$lt": [{
                                        "$size": {
                                            "$filter": {
                                                "input": "$task_state.worker_states",
                                                "as": "item",
                                                "cond": {"$or":[
                                                  { "$gt": ["$$item.ping_expire_time", "$$NOW"] },
                                                  { "$ne": ["$$item.success_time", Bson::Null] },
                                                  { "$ne": ["$$item.fail_time", Bson::Null] },
                                                ]}
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
            "$set": {
                "task_state.worker_states": {
                    "$concatArrays": [{
                        "$filter": {
                            "input": "$task_state.worker_states",
                            "as": "item",
                            "cond": {"$or":[
                              { "$gt": ["$$item.ping_expire_time", "$$NOW"] },
                              { "$ne": ["$$item.success_time", Bson::Null] },
                              { "$ne": ["$$item.fail_time", Bson::Null] },
                            ]}
                        }
                    }, [{
                        "worker_id": worker_id,
                        "ping_expire_time": "$$NOW",
                    }]]
                }
            }
        }];
        let mut update_options = FindOneAndUpdateOptions::default();
        update_options.sort = Some(doc! {"task_option.priority": -1});
        update_options.return_document = Some(ReturnDocument::After);
        // update_options.projection = Some( {"task_state": 1});
        match self.collection.find_one_and_update(
            filter, update, Some(update_options),
        ).await {
            Ok(Some(task)) => {
                Ok(task)
            }
            Ok(None) => {
                Err(NoTaskMatched)
            }
            Err(e) => {
                Err(MongoDbError(e.into()))
            }
        }
    }

    async fn gen_change_stream(&mut self) -> MResult<ChangeStream<ChangeStreamEvent<NextDoc>>> {
        let pipeline = [
            // only consider the task we can run
            Self::gen_pipeline(&self.config),
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
                Err(MongoDbError(e.into()))
            }
        }
    }

    fn gen_pipeline(config: &TaskConsumerConfig) -> Document {
        doc! {
            "$match":{
                "$and":[
                     {"task_state.worker_states":{"$exists":true}},
                     {
                        "$or":[
                            { "task_option.min_worker_version": { "$exists": false } },
                            { "task_option.min_worker_version": { "$lte": &config.worker_version } },
                        ]
                    },
                     {
                        "$or":[
                            { "task_option.specific_worker_ids": { "$exists": false } },
                            { "task_option.specific_worker_ids": { "$size": 0 } },
                            { "task_option.specific_worker_ids": config.get_worker_id() },
                        ]
                    },
                     {
                        "$or":[
                            { "task_state.worker_states": { "$size": 0 } },
                            {
                                "$expr": {
                                    "$lt": [{
                                        "$size": {
                                            "$filter": {
                                                "input": "$task_state.worker_states",
                                                "as": "item",
                                                "cond": { "$gt": ["$$item.ping_expire_time", "$$NOW"] }
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