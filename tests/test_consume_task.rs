use async_trait::async_trait;
use mscheduler::tasker::consumer::TaskConsumerFunc;
use mscheduler::tasker::error::MResult;

mod common;

struct TestConsumeFunc {}

#[async_trait]
impl TaskConsumerFunc<i32, i32> for TestConsumeFunc {
    async fn consumer(&self, params: i32) -> MResult<i32> {
        Ok(params)
    }
}

#[cfg(test)]
mod test {
    use mscheduler::tasker::consumer::TaskConsumer;

    use crate::common::get_collection;
    use crate::TestConsumeFunc;

    #[tokio::test]
    pub async fn test_consume_task() {
        let collection = get_collection("test_send_duplicate_task").await;
        let mut task_consumer = TaskConsumer::create(collection, TestConsumeFunc {}).await.unwrap();
        // task_consumer.start().await;
    }
}