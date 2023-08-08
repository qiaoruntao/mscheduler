use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
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
    fn run<T: Send + Clone + 'static, Func: TaskConsumerFunc<T, K>>(arc: Arc<Func>, params: Option<T>, sender: Sender<PostHandling>, key: String) -> TaskState<K> {
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
                if let Err(e) = sender.send(PostHandling { key: key.as_str().to_string() }).await {
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

pub struct SharedConsumerState<T: Send, K: Send, Func: TaskConsumerFunc<T, K>> {
    collection: Collection<Task<T, K>>,
    func: Arc<Func>,
    config: TaskConsumerConfig,
    task_map: Mutex<HashMap<String, TaskState<K>>>,
}

pub struct TaskConsumer<T: Send, K: Send, Func: TaskConsumerFunc<T, K>> {
    marker: PhantomData<Task<T, K>>,
    state: Arc<SharedConsumerState<T, K, Func>>,
}

#[derive(Deserialize)]
struct NextDoc {
    pub key: String,
    pub start_time: DateTime,
}

#[derive(Deserialize)]
struct PostHandling {
    pub key: String,
}

#[derive(Deserialize)]
struct FetchLoopDoc {}

impl<T: DeserializeOwned + Send + Unpin + Sync + Clone + 'static, K: Serialize + DeserializeOwned + Send + Unpin + Sync + 'static, Func: TaskConsumerFunc<T, K> + Send> TaskConsumer<T, K, Func> {
    pub async fn create(collection: Collection<Task<T, K>>, func: Func, config: TaskConsumerConfig) -> MResult<Self> {
        let shared_consumer_state = SharedConsumerState {
            collection,
            func: Arc::new(func),
            config,
            task_map: Mutex::new(Default::default()),
        };
        let consumer = TaskConsumer {
            marker: Default::default(),
            state: Arc::new(shared_consumer_state),
        };
        Ok(consumer)
    }

    pub fn add2queue(key: String, run_time: DateTime, queue: &Mutex<DelayQueue<String>>) {
        let diff = run_time.timestamp_millis() - DateTime::now().timestamp_millis();
        trace!("add key {} to wait queue",&key);
        if diff <= 0 {
            queue.lock().unwrap().insert(key, Duration::ZERO);
        } else {
            // diff max at about 2 years, we limit it to 1000 seconds
            queue.lock().unwrap().insert(key, Duration::from_millis(diff.min(1_000_000) as u64));
        }
    }

    pub async fn start(self: &Self) {
        let (next_doc_sender, next_doc_receiver) = tokio::sync::mpsc::channel(10);
        let (post_handling_sender, post_handling_receiver) = tokio::sync::mpsc::channel(10);
        let (fetch_loop_sender, fetch_loop_receiver) = tokio::sync::mpsc::channel(10);

        // start post handling process loop
        let handle1 = tokio::spawn(Self::start_post_running(self.state.clone(), post_handling_receiver));
        // listen to change stream to update occupy queue
        let handle2 = tokio::spawn(Self::start_listen2change_stream(self.state.clone(), next_doc_sender.clone()));
        // start task processing loop, this loop spawn actual task running closure
        let handle3 = tokio::spawn(Self::start_process_loop(self.state.clone(), next_doc_receiver, post_handling_sender.clone(), fetch_loop_sender.clone()));
        // start watch dog loop
        let handle4 = tokio::spawn(Self::start_task_fetcher(self.state.clone(), next_doc_sender.clone(), fetch_loop_receiver));
        let _ = tokio::join!(handle1, handle2, handle3, handle4);
    }


    async fn start_listen2change_stream(state: Arc<SharedConsumerState<T, K, Func>>, next_doc_sender: Sender<NextDoc>) {
        loop {
            trace!("start_listen2change_stream");
            let mut change_stream = Self::gen_change_stream(&state.config, &state.collection).await.unwrap();
            while let Some(result) = change_stream.next().await {
                match result {
                    Ok(change_event) => {
                        match change_event.full_document {
                            None => {
                                warn!("full document not provided");
                            }
                            Some(doc) => {
                                next_doc_sender.send(doc).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!("change stream failed {}",e);
                        break;
                    }
                }
            }
            warn!("change stream exits");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn start_process_loop(state: Arc<SharedConsumerState<T, K, Func>>, mut next_doc_receiver: Receiver<NextDoc>, post_handling_sender: Sender<PostHandling>, fetch_loop_sender: Sender<FetchLoopDoc>) {
        let mut queue = DelayQueue::<String>::new();
        loop {
            trace!("start_process_loop");
            tokio::select! {
                Some(expired)=futures::future::poll_fn(|cx| queue.poll_expired(cx))=>{
                    Self::try_occupy_task(state.clone(), expired, post_handling_sender.clone()).await;
                    if queue.is_empty(){
                        trace!("queue empty, check remaining tasks");
                        // emit signal to fetch more tasks
                        fetch_loop_sender.send(FetchLoopDoc{}).await;
                    }
                }
                Some(doc)=next_doc_receiver.recv()=>{
                    trace!("handling new doc");
                    let diff = doc.start_time.timestamp_millis() - DateTime::now().timestamp_millis();
                    let duration=if diff>0{
                        Duration::from_millis(diff.min(1_000_000) as u64)
                    }else{
                        Duration::ZERO
                    };
                    queue.insert(doc.key, duration);
                }
            }
        }
    }

    async fn start_task_fetcher(state: Arc<SharedConsumerState<T, K, Func>>, next_doc_sender: Sender<NextDoc>, mut fetch_loop_receiver: Receiver<FetchLoopDoc>) {
        trace!("start_task_fetcher");
        let filter = Self::gen_pipeline(&state.config);
        let pipeline = [filter];
        Self::fetch_more_task(state.clone(), next_doc_sender.clone(), pipeline.clone()).await;
        while let Some(_) = fetch_loop_receiver.recv().await {
            Self::fetch_more_task(state.clone(), next_doc_sender.clone(), pipeline.clone()).await;
        }
    }

    async fn fetch_more_task(state: Arc<SharedConsumerState<T, K, Func>>, next_doc_sender: Sender<NextDoc>, pipeline: [Document; 1]) {
        trace!("start fetch more docs");
        let mut cursor = state.collection.aggregate(pipeline.clone(), None).await.unwrap();
        // add first 10 tasks to queue
        for _ in 0..10 {
            if let Some(Ok(task)) = cursor.next().await {
                let task = from_document::<Task<T, K>>(task).unwrap();
                let next_doc = NextDoc { key: task.key, start_time: task.task_state.start_time };
                next_doc_sender.send(next_doc).await;
            } else {
                break;
            }
        }
    }

    async fn start_post_running(state: Arc<SharedConsumerState<T, K, Func>>, mut post_handling_receiver: Receiver<PostHandling>) {
        trace!("start_post_running");
        while let Some(PostHandling { key }) = post_handling_receiver.recv().await {
            trace!("start to post handling key {} worker_id {}", key, state.config.get_worker_id());
            // 1. check task state
            let task_state = match state.task_map.lock().unwrap().remove(&key) {
                None => {
                    error!("failed to get task state for key {}", &key);
                    continue;
                }
                Some(v) => { v }
            };
            // 2. update its state based on running result
            if !task_state.handler.is_finished() {
                warn!("task is not finished before post running, key={}",&key);
            }
            let handle_result = match task_state.handler.await.unwrap() {
                Ok(returns) => {
                    Self::mark_task_success(state.clone(), &key, returns).await
                }
                Err(e) => {
                    Self::mark_task_failed(state.clone(), &key, e).await
                }
            };
        }
    }

    // update worker state to success if not already success
    async fn mark_task_success(state: Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>, returns: K) -> MResult<()> {
        let query = doc! {
            "key":key.as_ref(),
            "task_state.worker_states.worker_id": state.config.get_worker_id()
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
        match state.collection.update_one(query, update, None).await {
            Ok(v) => {
                if v.modified_count == 0 {
                    error!("failed to set task {} to success", key.as_ref());
                    Err(NoTaskMatched)
                } else {
                    trace!("set task {} to success", key.as_ref());
                    Ok(())
                }
            }
            Err(e) => {
                error!("failed to set task {} to success, {}", key.as_ref(), &e);
                Err(MongoDbError(e.into()))
            }
        }
    }
    async fn mark_task_failed(state: Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>, e: MSchedulerError) -> MResult<()> {
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
                "key": key.as_ref(),
                "task_state.worker_states.worker_id": state.config.get_worker_id()
            };
        match state.collection.update_one(query, update, None).await {
            Ok(v) => {
                if v.modified_count == 0 {
                    error!("failed to set task {} to failed", key.as_ref());
                    Err(NoTaskMatched)
                } else {
                    trace!("set task {} to fail", key.as_ref());
                    Ok(())
                }
            }
            Err(e) => {
                error!("failed to set task {} to fail, {}", key.as_ref(), &e);
                Err(MongoDbError(e.into()))
            }
        }
    }

    async fn try_occupy_task(state: Arc<SharedConsumerState<T, K, Func>>, expired: Expired<String>, post_handling_sender: Sender<PostHandling>) {
        let deadline = expired.deadline();
        let key = expired.get_ref();
        if deadline + Duration::from_secs(100) < Instant::now() {
            warn!("task key {} expired long ago", key);
            return;
        }
        // otherwise we try to occupy this task
        match Self::occupy_task(state.clone(), key).await {
            Ok(task) => {
                // save task state and run it
                let params = task.params.clone();
                let arc = state.func.clone();
                let task_state = TaskState::run(arc, params, post_handling_sender, task.key.clone());
                trace!("success to occupy task key {}",&key);
                { state.task_map.lock().unwrap().insert(task.key.clone(), task_state); }
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

    async fn occupy_task(state: Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>) -> MResult<Task<T, K>> {
        let worker_id = state.config.get_worker_id();
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
                            { "task_option.min_worker_version": { "$lte": &state.config.get_worker_version() } },
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
        match state.collection.find_one_and_update(
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

    async fn gen_change_stream(config: &TaskConsumerConfig, collection: &Collection<Task<T, K>>) -> MResult<ChangeStream<ChangeStreamEvent<NextDoc>>> {
        let pipeline = [
            doc! {
                "$addFields":{
                    "task_state":"$fullDocument.task_state",
                    "task_option":"$fullDocument.task_option"
                }
            },
            // only consider the task we can run
            Self::gen_pipeline(config),
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
        match collection.clone_with_type::<NextDoc>().watch(pipeline, Some(change_stream_options)).await {
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
                    // not already occupied
                    {
                        "$expr": {
                        // running worker cnt+finished worker cnt<concurrent_worker_cnt
                            "$eq": [{
                                "$size": {
                                    "$filter": {
                                        "input": "$task_state.worker_states",
                                        "as": "item",
                                        "cond": { "$eq": ["$$item.worker_id", config.get_worker_id()] }
                                    }
                                }
                            }, 0]
                        }
                    },
                    // has worker space remains
                     {
                        "$or":[
                            { "task_state.worker_states": { "$size": 0 } },
                            {
                                "$expr": {
                                // running worker cnt+finished worker cnt<concurrent_worker_cnt
                                    "$lt": [{
                                        "$size": {
                                            "$filter": {
                                                "input": "$task_state.worker_states",
                                                "as": "item",
                                                "cond": {"$or":[
                                                    // running
                                                    {
                                                        "$and":[
                                                            { "$gt": ["$$item.ping_expire_time", "$$NOW"] },
                                                            { "$eq": ["$$item.success_time", Bson::Null] },
                                                            { "$eq": ["$$item.fail_time", Bson::Null] },
                                                        ]
                                                    },
                                                    // success
                                                    { "$ne": ["$$item.success_time", Bson::Null] },
                                                    // fail
                                                    { "$ne": ["$$item.fail_time", Bson::Null] }
                                                ]}
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