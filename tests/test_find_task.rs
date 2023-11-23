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

    use crate::common::get_collection_for_test;

    struct TestConsumeFunc {}

    #[async_trait]
    impl TaskConsumerFunc<i32, i32> for TestConsumeFunc {
        async fn consume(&self, params: Option<i32>) -> MResult<i32> {
            Ok(params.unwrap_or(0))
        }
    }

    #[test_log::test(tokio::test)]
    pub async fn test() {
        let collection: Collection<Task<i32, i32>> = get_collection_for_test("test_find_task").await;
        let collection2: Collection<Task<i32, i32>> = get_collection_for_test("test_find_task").await;
        // consumer
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        tokio::spawn({
            let task_consumer = task_consumer.clone();
            async move { task_consumer.start().await }
        });
        // listen to event
        let receiver = task_consumer.get_event_receiver();

        // test: wait for occupy event
        let wait4occupy_event_handler = tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                match event {
                    ConsumerEvent::WaitOccupy { key, .. } => {
                        info!("received key={}", &key);
                        return key == "111";
                    }
                    _ => {}
                }
            }
            false
        });
        // producer
        let task_producer = TaskProducer::create(collection2).expect("failed to create producer");


        // send task
        let mut send_task_option = SendTaskOption::default();
        send_task_option.concurrency_cnt = 2;
        task_producer.send_task("111", 1, Some(send_task_option)).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(5)).await;
        wait4occupy_event_handler.await;
        // check if ok
        // assert!(wait4occupy_event_handler.is_finished());
    }
}