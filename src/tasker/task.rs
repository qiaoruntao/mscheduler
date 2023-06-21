use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct TaskWorkerState<K> {
    // worker identifier
    pub worker_id: String,
    // how many unexpected retry has occurred
    pub unexpected_retry_cnt: Option<u32>,
    // when can other workers accept this task
    pub ping_expire_time: Option<DateTime>,
    // when did this task success
    pub success_time: Option<DateTime>,
    // when did this task failed
    pub fail_time: Option<DateTime>,
    // fail reason
    pub fail_reason: Option<String>,
    // task return values
    pub returns: Option<K>,
}

#[derive(Deserialize, Serialize)]
pub struct TaskState<K> {
    // when did this task created
    pub create_time: DateTime,
    // when should this task run
    pub start_time: DateTime,
    // worker ids of which are running this task
    pub worker_states: Vec<TaskWorkerState<K>>,
}

#[derive(Deserialize, Serialize)]
pub struct TaskOption {
    // priority
    pub priority: u32,
    // how many workers are allowed to run this task concurrently
    pub concurrent_worker_cnt: u32,
    // the interval to update worker running state
    pub ping_interval_ms: u32,
    // after which time other worker can replace timeout worker
    pub worker_timeout_ms: u32,
    // minimum worker version to run this task
    pub min_worker_version: u32,
    // only allow these workers to run this task
    pub specific_worker_ids: Vec<String>,
    // how many unexpected error are allowed to occur
    pub max_unexpected_retries: u32,
    // retry delay when unexpected error is occurred
    pub unexpected_retry_delay_ms: u32,
}

#[derive(Deserialize, Serialize)]
pub struct Task<T, K> {
    // identifier
    pub key: String,
    // record task running state
    pub task_state: TaskState<K>,
    // task running options
    pub task_option: TaskOption,
    // task parameters
    pub params: Option<T>,
}