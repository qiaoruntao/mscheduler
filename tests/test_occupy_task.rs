/// test whether task can be occupied as expected
mod common;

#[cfg(test)]
mod test {
    use std::time::Duration;

    use async_trait::async_trait;
    use log::info;
    use serde::{Deserialize, Serialize};
    use tokio::join;
    use tokio::sync::mpsc::channel;
    use tokio::time::timeout;
    use typed_builder::TypedBuilder;

    use mscheduler::tasker::consumer::{ConsumerEvent, TaskConsumer, TaskConsumerConfig, TaskConsumerFunc};
    use mscheduler::tasker::error::MResult;
    use mscheduler::tasker::error::MSchedulerError::ExecutionError;
    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};
    use mscheduler::tasker::task::Task;

    use crate::common::{get_collection_for_test, spawn_check_handler, spawn_running_consumer_handler};

    struct TestConsumeFunc {}

    #[derive(Default, TypedBuilder, Clone, Deserialize, Serialize)]
    #[builder(field_defaults(default, setter(into)))]
    #[non_exhaustive]
    struct TestConsumeParam {
        pub timeout_sec: u32,
        pub emit_error: bool,
    }

    #[async_trait]
    impl TaskConsumerFunc<TestConsumeParam, i32> for TestConsumeFunc {
        async fn consume(&self, params: Option<TestConsumeParam>) -> MResult<i32> {
            let param = params.unwrap_or_default();
            tokio::time::sleep(Duration::from_secs(param.timeout_sec as u64)).await;
            if param.emit_error {
                Err(ExecutionError(Box::new("emit error now".to_string())))
            } else {
                Ok(param.timeout_sec as i32)
            }
        }
    }

    #[test_log::test(tokio::test)]
    pub async fn test_occupy_task() {
        let collection_name = "test_occupy_task";

        // consumer
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(get_collection_for_test(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        let task_producer = TaskProducer::create(get_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await).expect("failed to create producer");
        spawn_running_consumer_handler(task_consumer.clone());

        // listen to event
        let occupy_handle = spawn_check_handler(task_consumer.clone(), |event| {
            match event {
                ConsumerEvent::TaskOccupyResult { key, success } => {
                    key == "111" && *success
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(5));

        // send task
        task_producer.send_task("111", TestConsumeParam::default(), None).await.expect("failed to send task");

        // wait for task to be consumed
        let occupy_result = occupy_handle.await;
        info!("wait occupy completed");
        assert!(occupy_result.expect("failed to wait 4 event").is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_concurrent_occupy_task() {
        let collection_name = "test_concurrent_occupy_task";
        // consumer
        let worker_id1 = "aaa";
        let worker_id2 = "bbb";
        let worker_id3 = "ccc";
        let task_consumer = TaskConsumer::create(get_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        let task_consumer2 = TaskConsumer::create(get_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id2).build()).await.expect("failed to create consumer2");
        let task_consumer3 = TaskConsumer::create(get_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id3).build()).await.expect("failed to create consumer3");

        // producer
        let task_producer = TaskProducer::create(get_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await).expect("failed to create producer");

        spawn_running_consumer_handler(task_consumer.clone());
        spawn_running_consumer_handler(task_consumer2.clone());
        spawn_running_consumer_handler(task_consumer3.clone());

        // listen to event
        let occupy_handle = spawn_check_handler(task_consumer.clone(), |event| {
            match event {
                ConsumerEvent::TaskOccupyResult { key, success } => {
                    key == "111" && *success
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(5));
        let occupy2_handle = spawn_check_handler(task_consumer2.clone(), |event| {
            match event {
                ConsumerEvent::TaskOccupyResult { key, success } => {
                    key == "111" && *success
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(5));
        let occupy3_handle = spawn_check_handler(task_consumer3.clone(), |event| {
            match event {
                ConsumerEvent::TaskOccupyResult { key, success } => {
                    key == "111" && *success
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(5));

        // send task
        task_producer.send_task("111", TestConsumeParam::default(), Some(SendTaskOption::builder().concurrency_cnt(2_u32).build())).await.expect("failed to send task");

        // wait for task to be consumed
        let (occupy_result, occupy_result2, occupy_result3) = join!(occupy_handle, occupy2_handle, occupy3_handle);
        info!("wait occupy completed");

        let success_cnt = [occupy_result.expect("failed to wait").is_some(), occupy_result2.expect("failed to wait").is_some(), occupy_result3.expect("failed to wait").is_some()].iter()
            .filter(|&&is_success| is_success)
            .count();

        assert_eq!(success_cnt, 2, "success cnt should be 2");
    }

    #[test_log::test(tokio::test)]
    pub async fn test_max_retry_count() {
        let collection_name = "test_max_retry_count";

        // consumer
        let worker_id1 = "aaa";
        let config = TaskConsumerConfig::builder().worker_id(worker_id1).build();
        let task_consumer = TaskConsumer::create(get_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, config).await.expect("failed to create consumer");

        // producer
        let task_producer = TaskProducer::create(get_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await).expect("failed to create producer");

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
        // tokio::time::sleep(Duration::from_secs(7)).await;

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
}