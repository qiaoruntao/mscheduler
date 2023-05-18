pub struct TaskConsumerFunc {}

impl TaskConsumerFunc {
    pub async fn consumer() {}
}

pub struct TaskConsumerConfig {
    // specific this worker's version, used to choose which task to run
    worker_version: Option<u32>,
    // specific this worker's id, used to remote control worker behavior, also can be used to choose which task to run
    worker_id: Option<String>,
    // whether this worker should continue to try to accept tasks
    allow_consume: bool,

}

pub struct TaskConsumer {}

impl TaskConsumer {
    pub async fn update_config(&self, config: TaskConsumerConfig) {}
    pub async fn start(&self) {}
}