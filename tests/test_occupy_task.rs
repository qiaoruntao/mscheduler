/// test whether task can be occupied as expected
mod common;

#[cfg(test)]
mod test {
    use std::time::Duration;

    use log::info;
    use tokio::join;
    use tracing::trace;

    use mscheduler::tasker::consumer::{ConsumerEvent, TaskConsumer, TaskConsumerConfig};
    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};
    use mscheduler::tasker::task::Task;

    use crate::common::{init_collection_for_test, spawn_check_handler, spawn_running_consumer_handler};
    use crate::common::test_consume_func::{TestConsumeFunc, TestConsumeParam};

    #[test_log::test(tokio::test)]
    pub async fn test_occupy_task() {
        let collection_name = "test_occupy_task";

        // consumer
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(init_collection_for_test(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        let task_producer = TaskProducer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await).expect("failed to create producer");
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
        let task_consumer = TaskConsumer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        let task_consumer2 = TaskConsumer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id2).build()).await.expect("failed to create consumer2");
        let task_consumer3 = TaskConsumer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id3).build()).await.expect("failed to create consumer3");

        // producer
        let task_producer = TaskProducer::create(init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await).expect("failed to create producer");

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
    pub async fn test_occupy_existing_task() {
        let collection_name = "test_occupy_existing_task";
        let collection1 = init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await;
        let collection2 = init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await;

        // producer
        let task_producer = TaskProducer::create(collection1).expect("failed to create producer");

        // send task
        let send_task_option = SendTaskOption::builder().concurrency_cnt(1_u32).worker_timeout_ms(3000_u32).build();
        task_producer.send_task("111", TestConsumeParam::builder().timeout_sec(2_u32).emit_error(true).build(), Some(send_task_option)).await.expect("failed to send task");

        // consumer
        let worker_id1 = "aaa";
        let config = TaskConsumerConfig::builder().worker_id(worker_id1).build();
        let task_consumer = TaskConsumer::create(collection2, TestConsumeFunc {}, config).await.expect("failed to create consumer");

        // wait for occupy event
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

        // start the producer
        spawn_running_consumer_handler(task_consumer.clone());

        // wait for task to consumed and success
        assert!(occupy_handle.await.expect("failed to wait").is_some(), "cannot find success event");
    }

    #[test_log::test(tokio::test)]
    pub async fn test_occupy_expired_task() {
        let collection_name = "test_occupy_expired_task";
        let collection1 = init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await;
        let collection2 = init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await;
        let collection3 = init_collection_for_test::<Task<TestConsumeParam, i32>>(collection_name).await;

        {
            // producer
            let task_producer = TaskProducer::create(collection1).expect("failed to create producer");

            // send task
            let send_task_option = SendTaskOption::builder().concurrency_cnt(1_u32).ping_interval_ms(1_u32).worker_timeout_ms(5_u32).build();
            // task timeout long enough
            task_producer.send_task("111", TestConsumeParam::builder().timeout_sec(100_u32).emit_error(true).build(), Some(send_task_option)).await.expect("failed to send task");
        }

        // consumer
        let worker_id1 = "aaa";
        let task_consumer1 = TaskConsumer::create(collection2, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build().clone()).await.expect("failed to create consumer");

        // wait for occupy event
        let occupy_handle = spawn_check_handler(task_consumer1.clone(), |event| {
            match event {
                ConsumerEvent::TaskOccupyResult { key, success } => {
                    key == "111" && *success
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(5));

        // start the producer
        let consumer_handler = spawn_running_consumer_handler(task_consumer1.clone());

        // stop consumer immediately after task occupied
        occupy_handle.await.expect("failed to occupy task").expect("task not occupied");
        drop(consumer_handler);

        // wait for the task to expire
        tokio::time::sleep(Duration::from_secs(7)).await;

        // start consumer 2 and wait it to occupy task

        let worker_id2 = "bbb";
        let task_consumer2 = TaskConsumer::create(collection3, TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id2).build()).await.expect("failed to create consumer");

        let occupy_handle2 = spawn_check_handler(task_consumer2.clone(), |event| {
            trace!("event={}",&event);
            match event {
                ConsumerEvent::TaskOccupyResult { key, success } => {
                    key == "111" && *success
                }
                _ => {
                    false
                }
            }
        }, Duration::from_secs(7));
        spawn_running_consumer_handler(task_consumer2.clone());
        occupy_handle2.await.expect("failed to occupy task by 2").expect("task not occupied by 2");
    }
}