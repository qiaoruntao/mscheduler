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
    use tracing::info;
    use mscheduler::tasker::consumer::{TaskConsumer, TaskConsumerConfig};
    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};

    use crate::common::get_collection;
    use crate::TestConsumeFunc;

    #[test_log::test(tokio::test)]
    pub async fn test_consume_task() {
        let collection = get_collection("test_consume_task").await;
        let consume_func = TestConsumeFunc {};
        let worker_id = "aaa";
        let mut task_consumer = TaskConsumer::create(collection.clone(), consume_func, TaskConsumerConfig {
            worker_version: 0,
            worker_id: worker_id.to_string(),
            allow_consume: true,
        }).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer.send_task("111", 1, None).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        assert_eq!(worker_state.worker_id, worker_id);
        assert_eq!(worker_state.unexpected_retry_cnt, Some(0));
        assert_eq!(worker_state.success_time, None);
    }

    #[test_log::test(tokio::test)]
    pub async fn test_multiple_consume_task() {
        info!("test");
        let collection = get_collection("test_multiple_consume_task").await;
        let worker_id1 = "aaa";
        let mut task_consumer = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig {
            worker_version: 0,
            worker_id: worker_id1.to_string(),
            allow_consume: true,
        }).await.expect("failed to create consumer");
        let worker_id2 = "bbb";
        let mut task_consumer2 = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig {
            worker_version: 0,
            worker_id: worker_id2.to_string(),
            allow_consume: true,
        }).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        tokio::spawn(async move { task_consumer2.start().await });
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        let mut send_task_option = SendTaskOption::default();
        send_task_option.concurrency_cnt = 2;
        task_producer.send_task("111", 1, Some(send_task_option)).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(3)).await;
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 2);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        // assert_eq!(worker_state.worker_id, worker_id1);
        assert_eq!(worker_state.unexpected_retry_cnt, Some(0));
        assert_eq!(worker_state.success_time, None);
    }
}