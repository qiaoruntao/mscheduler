use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use mongodb::bson::{doc, DateTime, Document};
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::Collection;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use strum::Display;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::time::delay_queue::Expired;
use tokio_util::time::DelayQueue;
use tracing::{error, info, instrument, trace, warn};
use typed_builder::TypedBuilder;

use crate::tasker::consumer::ConsumerEvent::{
    MarkSuccess, TaskExecuteResult, TaskOccupyResult, WaitOccupy, WaitOccupyQueueEmpty,
};
use crate::tasker::error::MSchedulerError::{ExecutionError, MongoDbError, NoTaskMatched};
use crate::tasker::error::{MResult, MSchedulerError};
use crate::tasker::task::Task;

#[async_trait]
pub trait TaskConsumerFunc<T: Send, K: Send>: Send + Sync + 'static {
    async fn consume(&self, params: Option<T>) -> MResult<K>;
}

#[derive(Deserialize, TypedBuilder, Debug, Clone)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TaskConsumerConfig {
    // specific this worker's id, used to remote control worker behavior, also can be used to choose which task to run
    pub worker_id: String,
}

impl TaskConsumerConfig {
    pub fn get_worker_id(&self) -> &str {
        self.worker_id.as_str()
    }
}

pub struct SharedConsumerState<T: Send + Sync, K: Send + Sync, Func: TaskConsumerFunc<T, K>> {
    collection: Collection<Task<T, K>>,
    func: Arc<Func>,
    is_fully_scanned: AtomicBool,
    config: TaskConsumerConfig,
    max_allowed_task_cnt: AtomicU32,
    consumer_event_sender: Sender<ConsumerEvent>,
    task_map: Mutex<HashMap<String, (JoinHandle<MResult<K>>, String)>>,
}

pub struct TaskConsumer<T: Send + Sync, K: Send + Sync, Func: TaskConsumerFunc<T, K>> {
    marker: PhantomData<Task<T, K>>,
    state: Arc<SharedConsumerState<T, K, Func>>,
}

impl<T: Send + Sync, K: Send + Sync, Func: TaskConsumerFunc<T, K>> Clone
    for TaskConsumer<T, K, Func>
{
    fn clone(&self) -> Self {
        TaskConsumer {
            marker: Default::default(),
            state: self.state.clone(),
        }
    }
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
        success: bool,
    },
    /// send this event when task is marked as success
    MarkSuccess {
        key: String,
    },
}

const MAX_CHANNEL_CAPACITY: usize = 2 << 6;

impl<
        T: DeserializeOwned + Send + Unpin + Sync + Clone + 'static,
        K: Serialize + DeserializeOwned + Send + Unpin + Sync + 'static,
        Func: TaskConsumerFunc<T, K> + Send,
    > TaskConsumer<T, K, Func>
{
    pub fn get_running_task_cnt(&self) -> u32 {
        self.state
            .task_map
            .lock()
            .expect("failed to lock task_map")
            .len() as u32
    }
    pub fn set_max_worker_cnt(&self, max_worker_cnt: u32) {
        self.state
            .max_allowed_task_cnt
            .store(max_worker_cnt, SeqCst);
    }

    pub fn get_max_worker_cnt(&self) -> u32 {
        self.state.max_allowed_task_cnt.load(SeqCst)
    }

    pub fn get_event_receiver(&self) -> Receiver<ConsumerEvent> {
        self.state.consumer_event_sender.subscribe()
    }

    #[instrument(skip_all)]
    pub async fn create(
        collection: Collection<Task<T, K>>,
        func: Func,
        config: TaskConsumerConfig,
    ) -> MResult<Self> {
        // receiver is dropped as we will spawn new in tokio::spawn
        let (sender, _) = tokio::sync::broadcast::channel::<ConsumerEvent>(MAX_CHANNEL_CAPACITY);
        let shared_consumer_state = SharedConsumerState {
            collection,
            func: Arc::new(func),
            is_fully_scanned: Default::default(),
            config,
            max_allowed_task_cnt: AtomicU32::new(u32::MAX),
            consumer_event_sender: sender,
            task_map: Default::default(),
        };
        let consumer = TaskConsumer {
            marker: Default::default(),
            state: Arc::new(shared_consumer_state),
        };
        Ok(consumer)
    }

    #[instrument(skip(queue))]
    pub fn add2queue(key: String, run_time: DateTime, queue: &Mutex<DelayQueue<String>>) {
        let diff = run_time.timestamp_millis() - DateTime::now().timestamp_millis();
        trace!("add key {} to wait queue", &key);
        if diff <= 0 {
            queue.lock().unwrap().insert(key, Duration::ZERO);
        } else {
            // diff max at about 2 years, we limit it to 1000 seconds
            queue
                .lock()
                .unwrap()
                .insert(key, Duration::from_millis(diff.min(1_000_000) as u64));
        }
    }

    #[instrument(skip_all)]
    pub async fn wait_for_event<F: Fn(&ConsumerEvent) -> bool>(
        self: &Self,
        check: F,
    ) -> Option<ConsumerEvent> {
        while let Ok(event) = self.get_event_receiver().recv().await {
            if check(&event) {
                return Some(event);
            }
        }
        None
    }

    #[instrument(skip_all)]
    pub async fn wait_for_event_with_timeout<F: Fn(&ConsumerEvent) -> bool>(
        self: &Self,
        check: F,
        timeout: Duration,
    ) -> Option<ConsumerEvent> {
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

    #[instrument(skip_all)]
    pub async fn shutdown(self: &Self) {
        // disallow occupy new task, task_map will not change now
        self.set_max_worker_cnt(0);
        // so we can fetch all tasks
        let mutex_guard = self
            .state
            .task_map
            .lock()
            .expect("failed to get task map")
            .drain()
            .into_iter()
            .collect::<Vec<_>>();
        for (key, (_handler, running_id)) in mutex_guard.iter() {
            // TODO: fail with no time delay
            if let Err(e) = TaskConsumer::mark_task_fail(self.state.clone(), key, running_id).await
            {
                error!("failed to mark task as failed before shutdown {}", e);
            }
        }
        info!("consumer shutdown completed");
    }

    async fn spawn_occupy(state: Arc<SharedConsumerState<T, K, Func>>) {
        trace!("spawn_occupy");
        let mut receiver = state.consumer_event_sender.subscribe();
        let sender = state.consumer_event_sender.clone();
        let mut queue = DelayQueue::<String>::new();
        let mut key2queue_key = HashMap::new();
        loop {
            select! {
                Ok(consumer_event) = receiver.recv()=>{
                    match &consumer_event {
                        WaitOccupy { key, next_occupy_time } => {
                            let wait_ms = next_occupy_time.timestamp_millis() - DateTime::now().timestamp_millis();
                            let wait_ms = wait_ms.max(0);
                            let duration=Duration::from_millis(wait_ms as u64);
                            if let Some(k)=key2queue_key.get(key){
                                queue.reset(k, duration);
                            }else{
                                let queue_key=queue.insert(key.clone(), duration);
                                key2queue_key.insert(key.clone(), queue_key);
                            }
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
                    let max_allowed_task_cnt = state.max_allowed_task_cnt.load(SeqCst);
                    let task_cnt = state.task_map.lock().unwrap().len()as u32;
                    key2queue_key.remove(expired.get_ref());
                    if task_cnt<max_allowed_task_cnt {
                        let _=TaskConsumer::try_occupy_task(state.clone(), &expired).await;
                    }
                }
            }
        }
    }

    /// this function should have bounded running time
    /// 1. occupy operation should have a timeout
    /// 2. avoid blocking select loop
    #[instrument(skip_all)]
    async fn try_occupy_task(
        state: Arc<SharedConsumerState<T, K, Func>>,
        expired: &Expired<String>,
    ) -> MResult<()> {
        let task_key = expired.get_ref();
        // occupy task first
        let (task, running_id) =
            match TaskConsumer::occupy_task(state.clone(), task_key.as_str()).await {
                Ok(v) => v,
                Err(e) => {
                    return Err(e);
                }
            };
        // start running task and handle results in this function
        TaskConsumer::execute_task(state, task, running_id).await;
        Ok(())
    }

    #[instrument(skip(state, key,running_id,returns), fields(key = %key.as_ref(),running_id = %running_id.as_ref()))]
    async fn postprocess_task(
        state: Arc<SharedConsumerState<T, K, Func>>,
        key: impl AsRef<str>,
        returns: &MResult<K>,
        running_id: impl AsRef<str>,
    ) {
        match returns {
            Ok(_) => {
                let _ = TaskConsumer::mark_task_success(state, key, running_id).await;
            }
            Err(_) => {
                // make this worker retry a bit later than other workers
                let next_occupy_time =
                    DateTime::from_millis(DateTime::now().timestamp_millis() + 3_000);
                if let Err(e) = state.consumer_event_sender.send(WaitOccupy {
                    key: key.as_ref().to_string(),
                    next_occupy_time,
                }) {
                    error!("failed to notify retry occupy {}", e);
                }
                let _ = TaskConsumer::mark_task_fail(state, key, running_id).await;
            }
        }
    }

    /// no need to store result
    #[instrument(skip(state, key,running_id), fields(key = %key.as_ref(),running_id = %running_id.as_ref()))]
    async fn mark_task_success(
        state: Arc<SharedConsumerState<T, K, Func>>,
        key: impl AsRef<str>,
        running_id: impl AsRef<str>,
    ) -> MResult<Task<T, K>> {
        // the filter matches specific running task.
        let filter = Self::verify_matched_running_task(&state, &key, &running_id);
        let update = doc! {
            "$set":{
                "task_state.worker_states.$.success_time":DateTime::now(),
            }
        };
        let task = match state.collection.find_one_and_update(filter, update).await {
            Ok(Some(v)) => {
                trace!("mark as success completed key={}", key.as_ref());
                if let Err(e) = state.consumer_event_sender.send(MarkSuccess {
                    key: key.as_ref().to_string(),
                }) {
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
                return Err(MongoDbError(Arc::from(e)));
            }
        };
        Ok(task)
    }

    /// no need to store error reason now
    #[instrument(skip(state, key,running_id), fields(key = %key.as_ref(),running_id = %running_id.as_ref()))]
    async fn mark_task_fail(
        state: Arc<SharedConsumerState<T, K, Func>>,
        key: impl AsRef<str>,
        running_id: impl AsRef<str>,
    ) -> MResult<Task<T, K>> {
        // the filter should the specific running task.
        // however we loose the restriction to not fail or success
        let filter = Self::verify_matched_running_task(&state, &key, &running_id);
        let update = doc! {
            "$set":{
                "task_state.worker_states.$.fail_time":DateTime::now(),
            }
        };
        trace!("mark_task_fail {}", &filter);
        let task = match state.collection.find_one_and_update(filter, update).await {
            Ok(Some(v)) => {
                trace!("mark as failed completed key={}", key.as_ref());
                v
            }
            Ok(None) => {
                error!("failed to mark task as failed, cannot find that task");
                return Err(NoTaskMatched);
            }
            Err(e) => {
                error!("failed to mark task as failed, {}", &e);
                return Err(MongoDbError(Arc::from(e)));
            }
        };
        Ok(task)
    }

    #[instrument(skip_all, fields(key = %key.as_ref(),running_id = %running_id.as_ref()))]
    fn verify_matched_running_task(
        state: &Arc<SharedConsumerState<T, K, Func>>,
        key: impl AsRef<str>,
        running_id: impl AsRef<str>,
    ) -> Document {
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

    #[instrument(skip(state, task,running_id), fields(task.key = %task.key))]
    async fn execute_task(
        state: Arc<SharedConsumerState<T, K, Func>>,
        task: Task<T, K>,
        running_id: impl AsRef<str> + ToString,
    ) {
        let key = task.key;
        let running_id = running_id.as_ref().to_string();
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
                let mut interval =
                    tokio::time::interval(Duration::from_millis(ping_interval_ms as u64));
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    interval.tick().await;
                    match TaskConsumer::ping_task(
                        state.clone(),
                        &key,
                        &running_id,
                        worker_timeout_ms,
                    )
                    .await
                    {
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
        let consume_logic = {
            let key = key.clone();
            let state = state.clone();
            let running_id = running_id.clone();
            async move {
                trace!("start to consume task now key={}", &key);
                let result = state.func.consume(task.params).await;
                trace!("task consumed key={}", &key);
                // post-processing in this thread
                let _ =
                    TaskConsumer::postprocess_task(state.clone(), key.clone(), &result, running_id)
                        .await;
                // send event
                if let Err(e) = state.consumer_event_sender.send(TaskExecuteResult {
                    key: key.clone(),
                    success: result.is_ok(),
                }) {
                    error!("failed to send post process event {}", e);
                }
                result
            }
        };
        let execution_logic = {
            let key = key.clone();
            let state = state.clone();
            async move {
                let result_value = select! {
                    _=ping_logic=>{
                        Err(MSchedulerError::PanicError)
                    }
                    result=consume_logic=>{
                        result
                    }
                };
                state
                    .task_map
                    .lock()
                    .expect("failed to lock task_map")
                    .remove(&key);
                result_value
            }
        };
        let join_handle = tokio::spawn(execution_logic);
        state
            .task_map
            .lock()
            .expect("failed to lock task_map")
            .insert(key, (join_handle, running_id));
    }

    #[instrument(skip(state,key,running_id), fields(key = %key.as_ref(),running_id = %running_id.as_ref()))]
    async fn ping_task(
        state: Arc<SharedConsumerState<T, K, Func>>,
        key: impl AsRef<str>,
        running_id: impl AsRef<str>,
        worker_timeout_ms: u32,
    ) -> MResult<Task<T, K>> {
        let task_key = key.as_ref();
        let running_id = running_id.as_ref();
        let next_expire_time =
            DateTime::from_millis(DateTime::now().timestamp_millis() + worker_timeout_ms as i64);

        let filter = Self::verify_matched_running_task(&state, &task_key, &running_id);
        let update = doc! {
            "$set":{
                "task_state.worker_states.$.ping_expire_time":next_expire_time,
            }
        };
        match state.collection.find_one_and_update(filter, update).await {
            Ok(Some(v)) => {
                trace!("successfully ping task key={}", &task_key);
                Ok(v)
            }
            Ok(None) => {
                trace!(
                    "failed to occupy task key={} cannot get matched task",
                    task_key
                );
                // no need to report failed to compete with other workers
                Err(NoTaskMatched)
            }
            Err(e) => {
                if let Err(e) = state.consumer_event_sender.send(TaskOccupyResult {
                    key: task_key.to_string(),
                    success: false,
                }) {
                    error!("failed to send occupy success event {}", e);
                }
                error!("failed to occupy task {}", &e);
                Err(ExecutionError(Box::new(e)))
            }
        }
    }

    #[instrument(skip(state,task_key), fields(task_key = %task_key.as_ref()))]
    async fn occupy_task(
        state: Arc<SharedConsumerState<T, K, Func>>,
        task_key: impl AsRef<str>,
    ) -> MResult<(Task<T, K>, String)> {
        let worker_id = &state.config.worker_id;
        let task_key = task_key.as_ref();
        trace!("try_occupy_task now key={} {}", task_key, worker_id);
        let all_conditions = vec![
            // match key
            doc! {"key":task_key},
            // max retry check
            Self::verify_not_completely_failed(worker_id),
            // double occupy check
            Self::verify_double_occupy(worker_id),
            // concurrent limit check
            Self::verify_concurrent_limit_check(true),
        ];
        let expire_time = DateTime::from_millis(DateTime::now().timestamp_millis() + 10_000i64);
        // some information on how to push elements into array
        // https://stackoverflow.com/questions/37427610/mongodb-update-or-insert-object-in-array
        let running_id = DateTime::now().timestamp_millis().to_string();
        let update = vec![doc! {
            "$set": {
                "task_state.worker_states": {
                    "$concatArrays": [{
                        "$filter": {
                            "input": "$task_state.worker_states",
                            "as": "item",
                            "cond": {"$or":[
                              { "$gt": ["$$item.ping_expire_time", "$$NOW"] },
                              { "$ne": ["$$item.success_time", null] },
                              { "$ne": ["$$item.fail_time", null] },
                            ]}
                        }
                    }, [{
                        "running_id": &running_id,
                        "worker_id": worker_id,
                        "ping_expire_time": expire_time,
                        "success_time": null,
                        "fail_time": null,
                    }]]
                }
            }
        }];
        let filter = doc! {"$and":all_conditions};
        trace!("updating {}", filter);
        match state.collection.find_one_and_update(filter, update).await {
            Ok(Some(v)) => {
                trace!("successfully occupy task key={}", &task_key);
                if let Err(e) = state.consumer_event_sender.send(TaskOccupyResult {
                    key: task_key.to_string(),
                    success: true,
                }) {
                    error!("failed to send occupy success event {}", e);
                }
                Ok((v, running_id))
            }
            Ok(None) => {
                trace!(
                    "failed to occupy task key={} cannot get matched task",
                    task_key
                );
                // no need to report failed to compete with other workers
                Err(NoTaskMatched)
            }
            Err(e) => {
                if let Err(e) = state.consumer_event_sender.send(TaskOccupyResult {
                    key: task_key.to_string(),
                    success: false,
                }) {
                    error!("failed to send occupy success event {}", e);
                }
                error!("failed to occupy task {}", &e);
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
                        "$$item.success_time", null]
                    },
                    {
                        "$eq": [
                        "$$item.fail_time", null]
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
                        "$$item.success_time", null]
                    },
                    {
                        "$eq": [
                        "$$item.fail_time", null]
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

    /// if this passes, then no double occupy
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
                                { "$eq": ["$$item.success_time", null] },
                                { "$eq": ["$$item.fail_time", null] },
                                // and belong to this worker
                                { "$eq": ["$$item.worker_id", worker_id]  },
                            ]}
                        }
                    }
                }, 0]
            }
        }
    }

    /// if this passes, then this worker can run this task
    fn verify_allowed_worker_id(worker_id: &String) -> Document {
        doc! {
            "$or": [
                {
                    "task_option.specific_worker_ids": { "$eq": [] }
                },
                {
                    "task_option.specific_worker_ids": { "$in": [worker_id] }
                }
            ]
        }
    }

    /// if this passes, not completely failed
    /// NOTE: we only need to consider this worker, we cannot interface other worker's retry count
    fn verify_not_completely_failed(worker_id: &String) -> Document {
        doc! {
            "$expr": {
                "$lt": [{
                    "$size": {
                        "$filter": {
                            "input": "$task_state.worker_states",
                            "as": "item",
                            "cond": {"$and":[
                              { "$eq": ["$$item.worker_id", worker_id]  },
                              { "$ne": ["$$item.fail_time", null] },
                            ]}
                        }
                    }
                }, "$task_option.max_unexpected_retries"]
            }
        }
    }

    /// if this passes, then not completely success
    fn verify_not_completely_success() -> Document {
        // task should not be completely success
        doc! {
            "$expr": {
                "$ne": [{
                    "$size": {
                        "$filter": {
                            "input": "$task_state.worker_states",
                            "as": "item",
                            "cond": { "$ne": ["$$item.success_time", null] }
                        }
                    }
                }, "$task_option.max_unexpected_retries"]
            }
        }
    }

    #[instrument(skip(state))]
    async fn fetch_task(state: Arc<SharedConsumerState<T, K, Func>>) -> MResult<()> {
        let worker_id = &state.config.worker_id;

        let all_conditions = vec![
            // not completely success
            Self::verify_not_completely_success(),
            // check if worker id is allowed
            Self::verify_allowed_worker_id(worker_id),
            // max retry check
            Self::verify_not_completely_failed(worker_id),
            // double occupy check
            Self::verify_double_occupy(worker_id),
            // concurrent limit check
            Self::verify_concurrent_limit_check(false),
        ];
        let filter = doc! {"$and":all_conditions};
        trace!("fetch_task {}", &filter);
        let mut cursor = match state.collection.find(filter).await {
            Ok(v) => v,
            Err(e) => {
                error!("failed to fetch more tasks {}", e);
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
                    let event =
                        match TaskConsumer::<T, K, Func>::infer_consumer_event_from_task(task) {
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
                        error!("failed to add new scanned task {}", &e);
                    }
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(state))]
    async fn spawn_fetch_db(state: Arc<SharedConsumerState<T, K, Func>>) -> MResult<()> {
        trace!("spawn_fetch_db");
        let mut receiver = state.consumer_event_sender.subscribe();
        loop {
            match receiver.recv().await {
                Ok(WaitOccupyQueueEmpty) => {
                    if state.is_fully_scanned.load(SeqCst) {
                        continue;
                    }
                    let _ = TaskConsumer::fetch_task(state.clone()).await;
                }
                Ok(_) => {}
                Err(e) => {
                    error!("failed to receive event {}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(state))]
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
            // copy fields to root level so we can reuse verify logic
            doc! {
                "$addFields":{
                    "task_state":"$fullDocument.task_state",
                    "task_option":"$fullDocument.task_option"
                }
            },
            // only these events are required, complete list is https://www.mongodb.com/docs/manual/reference/change-events/#std-label-change-stream-output
            doc! {
                "$match":{
                    "operationType":{"$in":["insert","replace","update"]}
                }
            },
            // filter some unnecessary task updates
            doc! {
                "$match":{
                    "$and":[
                        // avoid try to occupy already occupied task
                        Self::verify_double_occupy(worker_id),
                        // check if worker id is allowed
                        Self::verify_allowed_worker_id(worker_id),
                        // not completely success, so we have a chance to occupy
                        Self::verify_not_completely_success(),
                        // not completely failed, so we have a chance to occupy
                        Self::verify_not_completely_failed(worker_id),
                    ]
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
            },
        ];
        let mut change_stream = match state
            .collection
            .clone_with_type::<Task<T, K>>()
            .watch()
            .pipeline(pipeline)
            .with_options(change_stream_options)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("failed to open change stream {}", e);
                return Err(MongoDbError(e.into()));
            }
        };

        // keep track if we checked all existing tasks, and whether we could listen to change stream only.
        // after change stream restarts, we assume some tasks may be updated during the restart gap
        state.is_fully_scanned.store(false, SeqCst);

        // send a fetch request to fill up some tasks
        let _ = event_sender.send(WaitOccupyQueueEmpty).map_err(|e| {
            error!(
                "failed to fill up task queue at the start of change stream {}",
                e
            )
        });
        info!("start to listen to change stream");

        // listen to change event and send them to next processing stage
        while let Some(event) = change_stream.next().await {
            let change_stream_event = match event {
                Ok(v) => v,
                Err(e) => {
                    error!("failed to get change stream event {}", e);
                    continue;
                }
            };
            let task = match change_stream_event.full_document {
                None => {
                    warn!("change stream has no document");
                    continue;
                }
                Some(v) => v,
            };
            trace!("stream found key ={}", &task.key);
            let consumer_event =
                match TaskConsumer::<T, K, Func>::infer_consumer_event_from_task(task) {
                    None => {
                        // this is normal
                        continue;
                    }
                    Some(v) => v,
                };
            if let Err(e) = event_sender.send(consumer_event) {
                error!("failed to send consumer event {}", e);
            }
        }
        error!("change stream exited");
        Ok(())
    }

    #[instrument(skip(task), fields(task_id=%task.key))]
    fn infer_consumer_event_from_task(task: Task<T, K>) -> Option<ConsumerEvent> {
        // TODO: multiple worker cnt, occupy now
        let mut all_fail = true;
        let mut max_time = task.task_state.create_time;
        for state in task.task_state.worker_states {
            if let Some(t) = state.ping_expire_time {
                max_time = max_time.max(t);
            }
            if state.fail_time.is_none() {
                all_fail = false;
            }
        }
        let next_occupy_time = if all_fail { DateTime::now() } else { max_time };
        let event = WaitOccupy {
            key: task.key,
            next_occupy_time,
        };
        Some(event)
    }
}
