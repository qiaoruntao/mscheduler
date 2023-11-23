use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use kanal::{AsyncReceiver, AsyncSender, bounded_async};
use mongodb::bson::{DateTime, doc, Document};
use mongodb::Collection;
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::select;
use tokio_util::time::delay_queue::Expired;
use tokio_util::time::DelayQueue;
use tracing::{error, info, trace, warn};
use typed_builder::TypedBuilder;

use crate::tasker::consumer::ConsumerEvent::WaitOccupyQueueEmpty;
use crate::tasker::error::{MResult, MSchedulerError};
use crate::tasker::task::Task;

#[async_trait]
pub trait TaskConsumerFunc<T: Send, K: Send>: Send + Sync + 'static {
    async fn consume(&self, params: Option<T>) -> MResult<K>;
}

#[derive(Deserialize, TypedBuilder, Debug)]
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
    consumer_event_sender: AsyncSender<ConsumerEvent>,
    consumer_event_receiver: AsyncReceiver<ConsumerEvent>,
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

#[derive(Deserialize)]
struct PostHandling {
    pub key: String,
}

/// used to notify both inner and outer receivers
#[derive(Deserialize)]
pub enum ConsumerEvent {
    WaitOccupy {
        key: String,
        next_occupy_time: DateTime,
    },
    WaitOccupyQueueEmpty,
}

impl<T: DeserializeOwned + Send + Unpin + Sync + Clone + 'static, K: Serialize + DeserializeOwned + Send + Unpin + Sync + 'static, Func: TaskConsumerFunc<T, K> + Send> TaskConsumer<T, K, Func> {
    pub fn get_running_task_cnt(&self) -> u32 {
        0
    }

    pub fn get_event_receiver(&self) -> AsyncReceiver<ConsumerEvent> {
        self.state.consumer_event_receiver.clone()
    }

    pub async fn create(collection: Collection<Task<T, K>>, func: Func, config: TaskConsumerConfig) -> MResult<Self> {
        let (sender, receiver) = bounded_async::<ConsumerEvent>(0);
        let shared_consumer_state = SharedConsumerState {
            collection,
            func: Arc::new(func),
            is_fully_scanned: Default::default(),
            config,
            max_allowed_task_cnt: AtomicU32::new(u32::MAX),
            consumer_event_sender: sender,
            consumer_event_receiver: receiver,
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
        TaskConsumer::<T, K, Func>::spawn_listen_db(self.state.clone()).await;
        // select! {
        //     _=TaskConsumer::<T, K, Func>::spawn_listen_db(self.state.clone())=>{
        //         warn!("listen_db loop exits");
        //     }
        //     _=TaskConsumer::<T, K, Func>::spawn_fetch_db(self.state.clone())=>{
        //         warn!("fetch_db loop exits");
        //     }
        //     _=TaskConsumer::<T, K, Func>::spawn_occupy(self.state.clone())=>{
        //         warn!("occupy loop exits");
        //     }
        // }
    }

    async fn spawn_occupy(state: Arc<SharedConsumerState<T, K, Func>>) {
        trace!("spawn_occupy");
        let receiver = state.consumer_event_receiver.clone();
        let sender = state.consumer_event_sender.clone();
        let mut queue = DelayQueue::<String>::new();
        loop {
            select! {
                Ok(consumer_event) = receiver.recv()=>{
                    match &consumer_event {
                        ConsumerEvent::WaitOccupy { key, next_occupy_time } => {
                            let wait_ms = next_occupy_time.timestamp_millis() - DateTime::now().timestamp_millis();
                            let wait_ms = wait_ms.max(0);
                            queue.insert(key.clone(), Duration::from_millis(wait_ms as u64));
                        }
                        _=>{}
                    }
                }
                Some(expired)=futures::future::poll_fn(|cx| queue.poll_expired(cx))=>{
                    if queue.is_empty(){
                        if let Err(e)=sender.send(WaitOccupyQueueEmpty).await{
                            error!("failed to send occupy queue empty event {}",e);
                        }
                    }
                    TaskConsumer::try_occupy_task(state.clone(), &expired).await;
                }
            }
        }
    }

    /// this function should have bounded running time
    /// 1. occupy operation should have a timeout
    /// 2. avoid blocking select loop
    async fn try_occupy_task(state: Arc<SharedConsumerState<T, K, Func>>, expired: &Expired<String>) {
        let task_key = expired.get_ref();
        trace!("try_occupy_task now key={}", task_key);
    }

    async fn spawn_fetch_db(state: Arc<SharedConsumerState<T, K, Func>>) -> MResult<()> {
        trace!("spawn_fetch_db");
        let receiver = state.consumer_event_receiver.clone();
        while let Ok(consumer_event) = receiver.recv().await {
            match consumer_event {
                WaitOccupyQueueEmpty => {
                    if state.is_fully_scanned.load(SeqCst) {
                        continue;
                    }
                    info!("start to fetch db");
                    // TODO: fetch db
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
            // check both running and created tasks,
            // Self::gen_pipeline(config, false),
            doc! {
                "$project":{
                    // _id cannot get filtered, will get error if filtered
                    "operationType":1_i32,
                    // mongodb-rust says ns field should not get filtered
                    "ns":1_i32,
                    // "fullDocument":1_i32,
                    "fullDocument.key":"$fullDocument.key",
                    "fullDocument.next_occupy_time":{"$max":["$fullDocument.task_state.start_time", {"$max":"$fullDocument.task_state.worker_states.ping_expire_time"}]},
                }
            }
        ];
        println!("{}", pipeline[0]);
        println!("{}", pipeline[1]);
        let mut change_stream = match state.collection.clone_with_type::<Document>().watch([], None).await {
            Ok(v) => { v }
            Err(e) => {
                error!("failed to open change stream {}",e);
                return Err(MSchedulerError::MongoDbError(e.into()));
            }
        };
        // keep track if we checked all existing tasks, and whether we could listen to change stream only.
        // after change stream restarts, we assume some tasks may be updated during the restart gap
        state.is_fully_scanned.store(false, Ordering::SeqCst);
        // send a fetch request to fill up some tasks
        // let _ = event_sender.send(WaitOccupyQueueEmpty).await
        //     .map_err(|e| error!("failed to fill up task queue at the start of change stream {}",e));
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
            info!("doc = {:?}", task);
            // let consumer_event = match TaskConsumer::<T, K, Func>::infer_consumer_event_from_task(task) {
            //     None => {
            //         // this is normal
            //         continue;
            //     }
            //     Some(v) => { v }
            // };
            // if let Err(e) = event_sender.send(consumer_event).await {
            //     error!("failed to send consumer event {}",e);
            // }
        }
        error!("change stream exited");
        Ok(())
    }

    fn infer_consumer_event_from_task(task: NextDoc) -> Option<ConsumerEvent> {
        // let mut max_time = task.task_state.create_time;
        // for state in task.task_state.worker_states {
        //     if let Some(t) = state.ping_expire_time {
        //         max_time = max_time.max(t);
        //     }
        // }
        let event = ConsumerEvent::WaitOccupy {
            key: task.key,
            next_occupy_time: task.next_occupy_time,
        };
        Some(event)
    }
}