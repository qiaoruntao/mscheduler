/// test running task
mod common;

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::sync::mpsc::channel;
    use tokio::time::timeout;

    use mscheduler::tasker::consumer::{ConsumerEvent, TaskConsumer, TaskConsumerConfig};
    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};
    use mscheduler::tasker::task::Task;

    use crate::common::test::{init_collection_for_test, spawn_check_handler, spawn_running_consumer_handler};
    use crate::common::test_consume_func::{TestConsumeFunc, TestConsumeParam};

    // test whether fail task is ok
    #[test_log::test(tokio::test)]
    pub async fn test_max_retry_count() {
        let collection_name = "test_max_retry_count";

        // consumer
        let worker_id1 = "aaa";
        let config = TaskConsumerConfig::builder().worker_id(worker_id1).build();
        let task_consumer = TaskConsumer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, config).await.expect("failed to create consumer");

        // producer
        let task_producer = TaskProducer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await).expect("failed to create producer");

        // start the producer
        spawn_running_consumer_handler(task_consumer.clone());

        // wait for multiple occupy event
        let (tx, mut rx) = channel(100);
        tokio::spawn({
            let mut receiver = task_consumer.get_event_receiver();
            async move {
                while let Ok(event) = receiver.recv().await {
                    match &event {
                        ConsumerEvent::WaitOccupy { .. } => {
                            tx.send(event).await.expect("failed to store event");
                        }
                        _ => {}
                    }
                }
            }
        });

        // send task
        let send_task_option = SendTaskOption::builder().concurrency_cnt(1_u32).worker_timeout_ms(3000_u32).build();
        task_producer.send_task("111", TestConsumeParam::builder().timeout_sec(2_u32).emit_error(true).build(), Some(send_task_option)).await.expect("failed to send task");

        // wait for task occupy 1
        let consumer_event01 = timeout(Duration::from_secs(5), rx.recv()).await.expect("should receive an event").expect("should contain an event");
        match consumer_event01 {
            ConsumerEvent::WaitOccupy { .. } => {}
            _ => {
                assert!(false, "failed to receive occupy event");
            }
        }

        // wait for task occupy 2
        let consumer_event02 = timeout(Duration::from_secs(5), rx.recv()).await.expect("should receive an event").expect("should contain an event");
        match consumer_event02 {
            ConsumerEvent::WaitOccupy { .. } => {}
            _ => {
                assert!(false, "failed to receive occupy event");
            }
        }

        // wait for task occupy 3
        let consumer_event03 = timeout(Duration::from_secs(5), rx.recv()).await.expect("should receive an event").expect("should contain an event");
        match consumer_event03 {
            ConsumerEvent::WaitOccupy { .. } => {}
            _ => {
                assert!(false, "failed to receive occupy event");
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    /// test whether occupy task -> success task is ok
    #[test_log::test(tokio::test)]
    pub async fn test_success_task() {
        let collection_name = "test_success_task";

        // consumer
        let worker_id1 = "aaa";
        let config = TaskConsumerConfig::builder().worker_id(worker_id1).build();
        let task_consumer = TaskConsumer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, config).await.expect("failed to create consumer");

        // producer
        let task_producer = TaskProducer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await).expect("failed to create producer");

        // start the producer
        spawn_running_consumer_handler(task_consumer.clone());

        // check success event
        let success_handle = spawn_check_handler(task_consumer.clone(), |event| {
            match event {
                ConsumerEvent::MarkSuccess { key } => {
                    key == "111"
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(5));
        // send task
        let param = TestConsumeParam::builder().timeout_sec(1_u32).emit_error(false).build();
        task_producer.send_task("111", param, None).await.expect("failed to send task");

        // wait for task to consumed and success
        assert!(success_handle.await.expect("failed to wait").is_some(), "cannot find success event");
    }
}