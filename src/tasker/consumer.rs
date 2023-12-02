use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use async_trait::async_trait;
use futures::{future, FutureExt, pin_mut, StreamExt};
use futures::future::Either::{Left, Right};
use mongodb::bson::{DateTime, doc, Document};
use mongodb::bson::Bson;
use mongodb::Collection;
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use strum::Display;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::time::delay_queue::Expired;
use tokio_util::time::DelayQueue;
use tracing::{error, info, trace, warn};
use typed_builder::TypedBuilder;

use crate::tasker::consumer::ConsumerEvent::{MarkSuccess, TaskExecuteResult, TaskOccupyResult, WaitOccupy, WaitOccupyQueueEmpty};
use crate::tasker::error::{MResult, MSchedulerError};
use crate::tasker::error::MSchedulerError::{ExecutionError, MongoDbError, NoTaskMatched};
use crate::tasker::task::Task;

#[async_trait]
pub trait TaskConsumerFunc<T: Send, K: Send>: Send + Sync + 'static {
    async fn consume(&self, params: Option<T>) -> MResult<K>;
}

#[derive(Deserialize, TypedBuilder, Debug, Clone)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TaskConsumerConfig {
    // specific this worker's version, used to choose which task to run
    #[builder(default = 0)]
    pub worker_version: u32,
    // specific this worker's id, used to remote control worker behavior, also can be used to choose which task to run
    pub worker_id: String,
}

impl TaskConsumerConfig {
    pub fn get_worker_version(&self) -> u32 {
        self.worker_version
    }

    pub fn get_worker_id(&self) -> &str {
        self.worker_id.as_str()
    }
}

pub struct SharedConsumerState<T: Send, K: Send, Func: TaskConsumerFunc<T, K>> {
    collection: Collection<Task<T, K>>,
    func: Arc<Func>,
    is_fully_scanned: AtomicBool,
    config: TaskConsumerConfig,
    max_allowed_task_cnt: AtomicU32,
    consumer_event_sender: Sender<ConsumerEvent>,
    consumer_event_receiver: Receiver<ConsumerEvent>,
    task_map: Mutex<HashMap<String, JoinHandle<MResult<K>>>>,
}

pub struct TaskConsumer<T: Send, K: Send, Func: TaskConsumerFunc<T, K>> {
    marker: PhantomData<Task<T, K>>,
    state: Arc<SharedConsumerState<T, K, Func>>,
}

impl<T: Send, K: Send, Func: TaskConsumerFunc<T, K>> Clone for TaskConsumer<T, K, Func> {
    fn clone(&self) -> Self {
        TaskConsumer {
            marker: Default::default(),
            state: self.state.clone(),
        }
    }
}

#[derive(Deserialize, Debug)]
struct NextDoc {
    pub key: String,
    pub next_occupy_time: DateTime,
}

/// used to notify both inner and outer receivers
#[derive(Clone, Debug, Display)]
pub enum ConsumerEvent {
    WaitOccupy {
        key: String,
        next_occupy_time: DateTime,
    },
    WaitOccupyQueueEmpty,
    TaskOccupyResult {
        key: String,
        success: bool,
    },
    TaskExecuteResult {
        key: String,
    },
    /// send this event when task is marked as success
    MarkSuccess {
        key: String,
    },
}

impl<T: DeserializeOwned + Send + Unpin + Sync + Clone + 'static, K: Serialize + DeserializeOwned + Send + Unpin + Sync + 'static, Func: TaskConsumerFunc<T, K> + Send> TaskConsumer<T, K, Func> {
    pub fn get_running_task_cnt(&self) -> u32 {
        self.state.task_map.lock().expect("failed to lock task_map").len() as u32
    }

    pub fn get_event_receiver(&self) -> Receiver<ConsumerEvent> {
        self.state.consumer_event_sender.subscribe()
    }

    pub async fn create(collection: Collection<Task<T, K>>, func: Func, config: TaskConsumerConfig) -> MResult<Self> {
        // TODO: magic number
        let (sender, receiver) = tokio::sync::broadcast::channel::<ConsumerEvent>(10);
        let shared_consumer_state = SharedConsumerState {
            collection,
            func: Arc::new(func),
            is_fully_scanned: Default::default(),
            config,
            max_allowed_task_cnt: AtomicU32::new(u32::MAX),
            consumer_event_sender: sender,
            consumer_event_receiver: receiver,
            task_map: Default::default(),
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

    pub async fn wait_for_event<F: Fn(&ConsumerEvent) -> bool>(self: &Self, check: F) -> Option<ConsumerEvent> {
        while let Ok(event) = self.get_event_receiver().recv().await {
            if check(&event) {
                return Some(event);
            }
        }
        None
    }

    pub async fn wait_for_event_with_timeout<F: Fn(&ConsumerEvent) -> bool>(self: &Self, check: F, timeout: Duration) -> Option<ConsumerEvent> {
        select! {
            _=tokio::time::sleep(timeout)=>{
                None
            }
            result=self.wait_for_event(check)=>{
                result
            }
        }
    }

    pub async fn start(self: &Self) {
        // TaskConsumer::<T, K, Func>::spawn_listen_db(self.state.clone()).await;
        select! {
            _=TaskConsumer::<T, K, Func>::spawn_listen_db(self.state.clone())=>{
                warn!("listen_db loop exits");
            }
            _=TaskConsumer::<T, K, Func>::spawn_fetch_db(self.state.clone())=>{
                warn!("fetch_db loop exits");
            }
            _=TaskConsumer::<T, K, Func>::spawn_occupy(self.state.clone())=>{
                warn!("occupy loop exits");
            }
        }
    }

    async fn spawn_occupy(state: Arc<SharedConsumerState<T, K, Func>>) {
        trace!("spawn_occupy");
        let mut receiver = state.consumer_event_sender.subscribe();
        let sender = state.consumer_event_sender.clone();
        let mut queue = DelayQueue::<String>::new();
        loop {
            select! {
                Ok(consumer_event) = receiver.recv()=>{
                    match &consumer_event {
                        WaitOccupy { key, next_occupy_time } => {
                            let wait_ms = next_occupy_time.timestamp_millis() - DateTime::now().timestamp_millis();
                            let wait_ms = wait_ms.max(0);
                            queue.insert(key.clone(), Duration::from_millis(wait_ms as u64));
                        }
                        _=>{}
                    }
                }
                Some(expired)=futures::future::poll_fn(|cx| queue.poll_expired(cx))=>{
                    if queue.is_empty(){
                        if let Err(e)=sender.send(WaitOccupyQueueEmpty){
                            error!("failed to send occupy queue empty event {}",e);
                        }
                    }
                    let _=TaskConsumer::try_occupy_task(state.clone(), &expired).await;
                }
            }
        }
    }

    /// this function should have bounded running time
    /// 1. occupy operation should have a timeout
    /// 2. avoid blocking select loop
    async fn try_occupy_task(state: Arc<SharedConsumerState<T, K, Func>>, expired: &Expired<String>) -> MResult<()> {
        let task_key = expired.get_ref();
        // occupy task first
        let (task, running_id) = match TaskConsumer::occupy_task(state.clone(), task_key.as_str()).await {
            Ok(v) => {
                v
            }
            Err(e) => {
                return Err(e);
            }
        };
        // start running task and handle results in this function
        TaskConsumer::execute_task(state, task, running_id).await;
        Ok(())
    }

    async fn postprocess_task(state: Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>, returns: &MResult<K>, running_id: String) {
        match returns {
            Ok(_) => {
                let _ = TaskConsumer::mark_task_success(state, key, running_id).await;
            }
            Err(_) => {
                // make this worker retry a bit later than other workers
                let next_occupy_time = DateTime::from_millis(DateTime::now().timestamp_millis() + 3_000);
                if let Err(e) = state.consumer_event_sender.send(WaitOccupy { key: key.as_ref().to_string(), next_occupy_time }) {
                    error!("failed to notify retry occupy {}", e);
                }
                let _ = TaskConsumer::mark_task_fail(state, key, running_id).await;
            }
        }
    }

    /// no need to store result
    async fn mark_task_success(state: Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>, running_id: String) -> MResult<Task<T, K>> {
        // the filter matches specific running task.
        let filter = Self::verify_matched_running_task(&state, &key, &running_id);
        let update = doc! {
            "$set":{
                "task_state.worker_states.$.success_time":DateTime::now(),
            }
        };
        let task = match state.collection.find_one_and_update(filter, update, None).await {
            Ok(Some(v)) => {
                trace!("mark as success succeeded key={}",key.as_ref());
                if let Err(e) = state.consumer_event_sender.send(MarkSuccess { key: key.as_ref().to_string() }) {
                    error!("failed to send success event {}", &e);
                }
                v
            }
            Ok(None) => {
                error!("failed to mark task as success, cannot find that task");
                return Err(NoTaskMatched);
            }
            Err(e) => {
                error!("failed to mark task as success, cannot find that task");
                return Err(MSchedulerError::MongoDbError(Arc::from(e)));
            }
        };
        Ok(task)
    }

    /// no need to store error reason now
    async fn mark_task_fail(state: Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>, running_id: String) -> MResult<Task<T, K>> {
        // the filter should the specific running task.
        // however we loose the restriction to not fail or success
        let filter = Self::verify_matched_running_task(&state, &key, &running_id);
        let update = doc! {
            "$set":{
                "task_state.worker_states.$.fail_time":DateTime::now(),
            }
        };
        trace!("mark_task_fail {}", &filter);
        let task = match state.collection.find_one_and_update(filter, update, None).await {
            Ok(Some(v)) => {
                trace!("mark as failed succeed key={}",key.as_ref());
                v
            }
            Ok(None) => {
                error!("failed to mark task as failed, cannot find that task");
                return Err(NoTaskMatched);
            }
            Err(e) => {
                error!("failed to mark task as failed, {}",&e);
                return Err(MSchedulerError::MongoDbError(Arc::from(e)));
            }
        };
        Ok(task)
    }

    fn verify_matched_running_task(state: &Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>, running_id: impl AsRef<str>) -> Document {
        let filter = doc! {
            "key": key.as_ref(),
            "task_state.worker_states":{
                "$elemMatch": {
                    "running_id": running_id.as_ref(),
                    "worker_id": &state.config.worker_id,
                    "fail_time": {"$eq": null},
                    "success_time": {"$eq": null}
                }
            }
        };
        filter
    }

    async fn execute_task(state: Arc<SharedConsumerState<T, K, Func>>, task: Task<T, K>, running_id: String) {
        let key = task.key;
        let join_handle = tokio::spawn({
            let state = state.clone();
            let key = key.clone();

            let ping_logic = {
                let key = key.clone();
                let state = state.clone();
                let running_id = running_id.clone();
                let worker_timeout_ms = task.task_option.worker_timeout_ms;
                let ping_interval_ms = task.task_option.ping_interval_ms;
                // 3 to ensure not accidentally exit
                let max_fail_cnt = worker_timeout_ms.div_ceil(ping_interval_ms).max(3);
                let mut continuous_fail_cnt = 0;
                async move {
                    let mut interval = tokio::time::interval(Duration::from_millis(ping_interval_ms as u64));
                    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    loop {
                        interval.tick().await;
                        match TaskConsumer::ping_task(state.clone(), &key, &running_id, worker_timeout_ms).await {
                            Ok(_) => {}
                            Err(NoTaskMatched) => {
                                trace!("failed to find task to ping key={}", &key);
                                // cannot find matched task, exit immediately
                                return MResult::<()>::Err(NoTaskMatched);
                            }
                            Err(e) => {
                                // keep trying until thread meets
                                continuous_fail_cnt += 1;
                                if continuous_fail_cnt >= max_fail_cnt {
                                    error!("max ping retry encountered, exit now");
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
            };
            let main_processing_logic = async move {
                trace!("start to consume task now key={}", &key);
                let result = state.func.consume(task.params).await;
                trace!("task consumed key={}", &key);
                // post processing in this thread
                let _ = TaskConsumer::postprocess_task(state.clone(), key.clone(), &result, running_id).await;
                // send event
                if let Err(e) = state.consumer_event_sender.send(TaskExecuteResult { key: key.clone() }) {
                    error!("failed to send post process event {}",e);
                }
                result
            };
            let result_value = select! {
                _=ping_logic=>{
                    Err(MSchedulerError::PanicError)
                }
                result=main_processing_logic=>{
                    result
                }
            };
            async move {
                result_value
            }
        });
        state.task_map.lock().expect("failed to lock task_map").insert(key, join_handle);
    }

    async fn ping_task(state: Arc<SharedConsumerState<T, K, Func>>, key: impl AsRef<str>, running_id: impl AsRef<str>, worker_timeout_ms: u32) -> MResult<Task<T, K>> {
        let task_key = key.as_ref();
        let running_id = running_id.as_ref();
        let next_expire_time = DateTime::from_millis(DateTime::now().timestamp_millis() + worker_timeout_ms as i64);

        let filter = Self::verify_matched_running_task(&state, &task_key, &running_id);
        let update = doc! {
            "$set":{
                "task_state.worker_states.$.ping_expire_time":next_expire_time,
            }
        };
        match state.collection.find_one_and_update(filter, update, None).await {
            Ok(Some(v)) => {
                trace!("successfully ping task key={}", &task_key);
                Ok(v)
            }
            Ok(None) => {
                trace!("failed to occupy task key={} cannot get matched task", task_key);
                // no need to report failed to compete with other workers
                Err(NoTaskMatched)
            }
            Err(e) => {
                if let Err(e) = state.consumer_event_sender.send(TaskOccupyResult { key: task_key.to_string(), success: false }) {
                    error!("failed to send occupy success event {}",e);
                }
                error!("failed to occupy task {}",&e);
                Err(ExecutionError(Box::new(e)))
            }
        }
    }

    async fn occupy_task(state: Arc<SharedConsumerState<T, K, Func>>, task_key: impl AsRef<str>) -> MResult<(Task<T, K>, String)> {
        let worker_id = &state.config.worker_id;
        let task_key = task_key.as_ref();
        trace!("try_occupy_task now key={} {}", task_key, worker_id);
        let all_conditions = vec![
            // match key
            doc! {"key":task_key},
            // max retry check
            Self::verify_max_retry_cnt(worker_id),
            // double occupy check
            Self::verify_double_occupy(worker_id),
            // concurrent limit check
            Self::verify_concurrent_limit_check(true),
        ];
        let expire_time = DateTime::from_millis(DateTime::now().timestamp_millis() + 10_000i64);
        // some information on how to push elements into array
        // https://stackoverflow.com/questions/37427610/mongodb-update-or-insert-object-in-array
        let running_id = DateTime::now().timestamp_millis().to_string();
        let update = vec![
            doc! {
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
                            "running_id": &running_id,
                            "worker_id": worker_id,
                            "ping_expire_time": expire_time,
                            "success_time": Bson::Null,
                            "fail_time": Bson::Null,
                        }]]
                    }
                }
            }
        ];
        let filter = doc! {"$and":all_conditions};
        trace!("updating {}", filter);
        match state.collection.find_one_and_update(filter, update, None).await {
            Ok(Some(v)) => {
                trace!("successfully occupy task key={}", &task_key);
                if let Err(e) = state.consumer_event_sender.send(TaskOccupyResult { key: task_key.to_string(), success: true }) {
                    error!("failed to send occupy success event {}",e);
                }
                Ok((v, running_id))
            }
            Ok(None) => {
                trace!("failed to occupy task key={} cannot get matched task", task_key);
                // no need to report failed to compete with other workers
                Err(NoTaskMatched)
            }
            Err(e) => {
                if let Err(e) = state.consumer_event_sender.send(TaskOccupyResult { key: task_key.to_string(), success: false }) {
                    error!("failed to send occupy success event {}",e);
                }
                error!("failed to occupy task {}",&e);
                Err(ExecutionError(Box::new(e)))
            }
        }
    }

    fn verify_concurrent_limit_check(check_running: bool) -> Document {
        let mut conditions = vec![
            // success
            doc! {
                "$and": [
                    {
                        "$ne": [
                        "$$item.success_time", Bson::Null]
                    },
                    {
                        "$eq": [
                        "$$item.fail_time", Bson::Null]
                    },
                ]
            },
        ];
        // running
        if check_running {
            conditions.push(doc! {
                "$and": [
                    {
                        "$gt": [
                        "$$item.ping_expire_time", DateTime::now()]
                    },
                    {
                        "$eq": [
                        "$$item.success_time", Bson::Null]
                    },
                    {
                        "$eq": [
                        "$$item.fail_time", Bson::Null]
                    },
                ]
            });
        }
        doc! {
            "$expr": {
                "$lt": [{
                    "$size": {
                        "$filter": {
                            "input": "$task_state.worker_states",
                            "as": "item",
                            "cond": {"$or":conditions}
                        }
                    }
                }, "$task_option.concurrent_worker_cnt"]
            }
        }
    }

    fn verify_double_occupy(worker_id: &String) -> Document {
        doc! {
                "$expr": {
                    "$eq": [{
                        "$size": {
                            "$filter": {
                                "input": "$task_state.worker_states",
                                "as": "item",
                                "cond": {"$and":[
                                    // running
                                    { "$gt": ["$$item.ping_expire_time", DateTime::now()] },
                                    { "$eq": ["$$item.success_time", Bson::Null] },
                                    { "$eq": ["$$item.fail_time", Bson::Null] },
                                    // and belong to this worker
                                    { "$eq": ["$$item.worker_id", worker_id]  },
                                ]}
                            }
                        }
                    }, 0]
                }
            }
    }

    fn verify_max_retry_cnt(worker_id: &String) -> Document {
        doc! {
                "$expr": {
                    "$lt": [{
                        "$size": {
                            "$filter": {
                                "input": "$task_state.worker_states",
                                "as": "item",
                                "cond": {"$and":[
                                  { "$eq": ["$$item.worker_id", worker_id]  },
                                  { "$ne": ["$$item.fail_time", Bson::Null] },
                                ]}
                            }
                        }
                    }, "$task_option.max_unexpected_retries"]
                }
            }
    }

    async fn fetch_task(state: Arc<SharedConsumerState<T, K, Func>>) -> MResult<()> {
        let worker_id = &state.config.worker_id;

        let all_conditions = vec![
            // max retry check
            Self::verify_max_retry_cnt(worker_id),
            // double occupy check
            Self::verify_double_occupy(worker_id),
            // concurrent limit check
            Self::verify_concurrent_limit_check(false),
        ];
        let filter = doc! {"$and":all_conditions};
        trace!("fetch_task {}", &filter);
        let mut cursor = match state.collection.find(filter, None).await {
            Ok(v) => v,
            Err(e) => {
                error!("failed to fetch more tasks {}",e);
                return Err(MongoDbError(Arc::new(e)));
            }
        };
        for _ in 0..10 {
            match cursor.next().await {
                None => {
                    trace!("all remaining tasks scanned");
                    state.is_fully_scanned.store(true, SeqCst);
                    break;
                }
                Some(Err(e)) => {
                    error!("failed to get more existing tasks {}", e);
                    break;
                }
                Some(Ok(task)) => {
                    let event = match TaskConsumer::<T, K, Func>::infer_consumer_event_from_task(task) {
                        None => {
                            trace!("task scanned no event is inferred");
                            continue;
                        }
                        Some(v) => {
                            trace!("task scanned event is inferred {:?}", &v);
                            v
                        }
                    };
                    if let Err(e) = state.consumer_event_sender.send(event) {
                        error!("failed to add new scanned task {}",&e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn spawn_fetch_db(state: Arc<SharedConsumerState<T, K, Func>>) -> MResult<()> {
        trace!("spawn_fetch_db");
        let mut receiver = state.consumer_event_sender.subscribe();
        while let Ok(consumer_event) = receiver.recv().await {
            match consumer_event {
                WaitOccupyQueueEmpty => {
                    if state.is_fully_scanned.load(SeqCst) {
                        continue;
                    }
                    info!("start to fetch db");
                    let _ = TaskConsumer::fetch_task(state.clone()).await;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn spawn_listen_db(state: Arc<SharedConsumerState<T, K, Func>>) -> MResult<()> {
        trace!("spawn_listen_db");
        // clone a receiver for this session
        let event_sender = state.consumer_event_sender.clone();
        let worker_id = &state.config.worker_id;
        // open change stream
        let change_stream_options = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();
        let pipeline = [
            doc! {
                "$addFields":{
                    "task_state":"$fullDocument.task_state",
                    "task_option":"$fullDocument.task_option"
                }
            },
            // filter some unnecessary task updates
            doc! {
                "$match":{
                    // avoid try to occupy already occupied task
                    "$and":[Self::verify_double_occupy(worker_id)]
                }
            },
            doc! {
                "$project":{
                    // _id cannot get filtered, will get error if filtered
                    "operationType":1_i32,
                    // mongodb-rust says ns field should not get filtered
                    "ns":1_i32,
                    "fullDocument":1_i32,
                    // "fullDocument.key":"$fullDocument.key",
                    // "fullDocument.next_occupy_time":{"$max":["$fullDocument.task_state.start_time", {"$max":"$fullDocument.task_state.worker_states.ping_expire_time"}]},
                }
            }
        ];
        let mut change_stream = match state.collection.clone_with_type::<Task<T, K>>().watch(pipeline, Some(change_stream_options)).await {
            Ok(v) => { v }
            Err(e) => {
                error!("failed to open change stream {}",e);
                return Err(MSchedulerError::MongoDbError(e.into()));
            }
        };

        // keep track if we checked all existing tasks, and whether we could listen to change stream only.
        // after change stream restarts, we assume some tasks may be updated during the restart gap
        state.is_fully_scanned.store(false, SeqCst);

        // send a fetch request to fill up some tasks
        let _ = event_sender.send(WaitOccupyQueueEmpty)
            .map_err(|e| error!("failed to fill up task queue at the start of change stream {}",e));
        info!("start to listen to change stream");

        // listen to change event and send them to next processing stage
        while let Some(event) = change_stream.next().await {
            let change_stream_event = match event {
                Ok(v) => { v }
                Err(e) => {
                    error!("failed to get change stream event {}",e);
                    continue;
                }
            };
            let task = match change_stream_event.full_document {
                None => {
                    warn!("change stream has no document");
                    continue;
                }
                Some(v) => {
                    v
                }
            };
            info!("stream found key ={}",&task.key);
            let consumer_event = match TaskConsumer::<T, K, Func>::infer_consumer_event_from_task(task) {
                None => {
                    // this is normal
                    continue;
                }
                Some(v) => { v }
            };
            if let Err(e) = event_sender.send(consumer_event) {
                error!("failed to send consumer event {}",e);
            }
        }
        error!("change stream exited");
        Ok(())
    }

    fn infer_consumer_event_from_task(task: Task<T, K>) -> Option<ConsumerEvent> {
        let mut max_time = task.task_state.create_time;
        for state in task.task_state.worker_states {
            if let Some(t) = state.ping_expire_time {
                max_time = max_time.max(t);
            }
        }
        let event = WaitOccupy {
            key: task.key,
            next_occupy_time: max_time,
        };
        Some(event)
    }
}