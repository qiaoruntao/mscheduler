/// test whether consumer can find task info updates in db
mod common;

#[cfg(test)]
mod test {
    use std::time::Duration;

    use async_trait::async_trait;
    use mongodb::Collection;
    use tracing::info;

    use mscheduler::tasker::consumer::{ConsumerEvent, TaskConsumer, TaskConsumerConfig, TaskConsumerFunc};
    use mscheduler::tasker::error::MResult;
    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};
    use mscheduler::tasker::task::Task;

    use crate::common::{init_collection_for_test, spawn_check_handler, spawn_running_consumer_handler};

    struct TestConsumeFunc {}

    #[async_trait]
    impl TaskConsumerFunc<i32, i32> for TestConsumeFunc {
        async fn consume(&self, params: Option<i32>) -> MResult<i32> {
            Ok(params.unwrap_or(0))
        }
    }

    #[test_log::test(tokio::test)]
    pub async fn test() {
        let collection: Collection<Task<i32, i32>> = init_collection_for_test("test_find_task").await;
        let collection2: Collection<Task<i32, i32>> = init_collection_for_test("test_find_task").await;
        // consumer
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        spawn_running_consumer_handler(task_consumer.clone());
        // test: wait for occupy event
        let wait4occupy_event_handler = spawn_check_handler(task_consumer.clone(), |event| {
            match event {
                ConsumerEvent::WaitOccupy { key, .. } => {
                    info!("received key={}", &key);
                    key == "111"
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(5));
        // producer
        let task_producer = TaskProducer::create(collection2).expect("failed to create producer");

        // send task
        let send_task_option = SendTaskOption::builder()
            .concurrency_cnt(2_u32)
            .build();
        task_producer.send_task("111", 1, Some(send_task_option)).await.expect("failed to send task");
        // check if ok
        assert!(wait4occupy_event_handler.await.expect("failed to wait 4 event").is_some());
    }
}