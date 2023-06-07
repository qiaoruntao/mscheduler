use async_trait::async_trait;
use mscheduler::tasker::consumer::TaskConsumerFunc;
use mscheduler::tasker::error::MResult;

mod common;

struct TestConsumeFunc {}

#[async_trait]
impl TaskConsumerFunc<i32, i32> for TestConsumeFunc {
    async fn consumer(&self, params: Option<i32>) -> MResult<i32> {
        Ok(params.unwrap_or(0))
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use mongodb::bson::doc;
    use mscheduler::tasker::consumer::TaskConsumer;
    use mscheduler::tasker::producer::TaskProducer;

    use crate::common::get_collection;
    use crate::TestConsumeFunc;

    #[tokio::test]
    pub async fn test_consume_task() {
        let collection = get_collection("test_consume_task").await;
        let consume_func = TestConsumeFunc {};
        let mut task_consumer = TaskConsumer::create(collection.clone(), consume_func).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer.send_task("111", 1, None).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
    }
}