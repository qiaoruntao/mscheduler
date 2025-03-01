use std::time::Duration;

use async_trait::async_trait;
use tracing::info;

use mscheduler::tasker::consumer::TaskConsumerFunc;
use mscheduler::tasker::error::MResult;
use mscheduler::tasker::error::MSchedulerError::ExecutionError;

mod common;

struct TestConsumeFunc {}

#[async_trait]
impl TaskConsumerFunc<i32, i32> for TestConsumeFunc {
    async fn consume(&self, params: Option<i32>) -> MResult<i32> {
        Ok(params.unwrap_or(0))
    }
}

struct TestStringConsumeFunc {}

#[async_trait]
impl TaskConsumerFunc<String, usize> for TestStringConsumeFunc {
    async fn consume(&self, params: Option<String>) -> MResult<usize> {
        Ok(params.map(|v| v.len()).unwrap_or(0))
    }
}

struct TestConsumeFailFunc {}

#[async_trait]
impl TaskConsumerFunc<i32, i32> for TestConsumeFailFunc {
    async fn consume(&self, _params: Option<i32>) -> MResult<i32> {
        let x = Box::new("".to_string());
        Err(ExecutionError(x))
    }
}

struct TestConsumeWithTimeParamFunc {}

#[async_trait]
impl TaskConsumerFunc<i64, i64> for TestConsumeWithTimeParamFunc {
    async fn consume(&self, time: Option<i64>) -> MResult<i64> {
        if let Some(wait_time) = time {
            info!("start to wait {} seconds", wait_time.abs());
            tokio::time::sleep(Duration::from_secs(wait_time.abs() as u64)).await;
        }
        info!("task consumed");

        let value = time.unwrap_or(0);
        if value > 0 {
            Ok(value)
        } else {
            Err(ExecutionError(Box::new(format!(
                "minus value as error {}",
                value
            ))))
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use mongodb::bson::doc;

    use mscheduler::tasker::consumer::{ConsumerEvent, TaskConsumer, TaskConsumerConfig};
    #[allow(unused_imports)]
    use mscheduler::tasker::error::MSchedulerError;
    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};

    use crate::common::test::{init_collection_for_test, spawn_check_handler};
    use crate::{
        TestConsumeFailFunc, TestConsumeFunc, TestConsumeWithTimeParamFunc, TestStringConsumeFunc,
    };

    #[test_log::test(tokio::test)]
    pub async fn test_consume_task() {
        let collection = init_collection_for_test("test_consume_task").await;
        let consume_func = TestConsumeFunc {};
        let worker_id = "aaa";
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            consume_func,
            TaskConsumerConfig::builder().worker_id(worker_id).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer
            .send_task("111", 1, None)
            .await
            .expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let task = collection
            .find_one(doc! {"key":"111"})
            .await
            .expect("failed to find")
            .expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        assert_eq!(worker_state.worker_id, worker_id);
        assert!(worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_string_task() {
        let collection = init_collection_for_test("test_consume_string_task").await;
        let consume_func = TestStringConsumeFunc {};
        let worker_id = "aaa";
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            consume_func,
            TaskConsumerConfig::builder().worker_id(worker_id).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer
            .send_task("111", "test".to_string(), None)
            .await
            .expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let task = collection
            .find_one(doc! {"key":"111"})
            .await
            .expect("failed to find")
            .expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        assert_eq!(worker_state.worker_id, worker_id);
        assert!(worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_fail_task() {
        let collection = init_collection_for_test("test_consume_fail_task").await;
        let consume_func = TestConsumeFailFunc {};
        let worker_id = "aaa";
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            consume_func,
            TaskConsumerConfig::builder().worker_id(worker_id).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer
            .send_task("111", 1, None)
            .await
            .expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let task = collection
            .find_one(doc! {"key":"111"})
            .await
            .expect("failed to find")
            .expect("no task found");
        // default retry cnt is 3
        assert_eq!(task.task_state.worker_states.len(), 3);
        for worker_state in task.task_state.worker_states {
            assert_eq!(worker_state.worker_id, worker_id);
            assert_eq!(worker_state.success_time, None);
            assert!(worker_state.fail_time.is_some());
        }
    }

    #[test_log::test(tokio::test)]
    pub async fn test_multiple_consume_task() {
        let collection = init_collection_for_test("test_multiple_consume_task").await;
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            TestConsumeFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id1).build(),
        )
        .await
        .expect("failed to create consumer");
        let worker_id2 = "bbb";
        let task_consumer2 = TaskConsumer::create(
            collection.clone(),
            TestConsumeFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id2).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        tokio::spawn(async move { task_consumer2.start().await });
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        let mut send_task_option = SendTaskOption::builder().build();
        send_task_option.concurrency_cnt = 2;
        task_producer
            .send_task("111", 1, Some(send_task_option))
            .await
            .expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(3)).await;
        let task = collection
            .find_one(doc! {"key":"111"})
            .await
            .expect("failed to find")
            .expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 2);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        // assert_eq!(worker_state.worker_id, worker_id1);
        assert!(worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_partial_success_multiple_consume_task() {
        let collection =
            init_collection_for_test("test_partial_success_multiple_consume_task").await;
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            TestConsumeFailFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id1).build(),
        )
        .await
        .expect("failed to create consumer");
        let worker_id2 = "bbb";
        let task_consumer2 = TaskConsumer::create(
            collection.clone(),
            TestConsumeFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id2).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        tokio::spawn(async move { task_consumer2.start().await });
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        let mut send_task_option = SendTaskOption::builder().build();
        send_task_option.concurrency_cnt = 2;
        task_producer
            .send_task("111", 1, Some(send_task_option))
            .await
            .expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(3)).await;
        let task = collection
            .find_one(doc! {"key":"111"})
            .await
            .expect("failed to find")
            .expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 2);
        let success_worker_state = task
            .task_state
            .worker_states
            .iter()
            .filter(|s| s.success_time.is_some())
            .next();
        assert!(success_worker_state.is_some());
        let success_worker_state = success_worker_state.unwrap();
        let fail_worker_state = task
            .task_state
            .worker_states
            .iter()
            .filter(|s| s.fail_time.is_some())
            .next();
        assert!(fail_worker_state.is_some());
        let fail_worker_state = fail_worker_state.unwrap();
        assert!(success_worker_state.success_time.is_some());
        assert!(fail_worker_state.fail_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_task_worker_id() {
        let collection = init_collection_for_test("test_consume_task_worker_id").await;
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            TestConsumeFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id1).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });

        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        let send_task_option = SendTaskOption::builder()
            .concurrency_cnt(1_u32)
            .specific_worker_ids(vec!["bbb".to_string()])
            .build();
        task_producer
            .send_task("111", 1, Some(send_task_option))
            .await
            .expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(1)).await;
        // task should not be consumed
        let task = collection
            .find_one(doc! {"key":"111"})
            .await
            .expect("failed to find")
            .expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 0);
        // spawn a matched consumer
        let worker_id2 = "bbb";
        let config2 = TaskConsumerConfig::builder().worker_id(worker_id2).build();
        assert_eq!(config2.get_worker_id(), worker_id2);
        let task_consumer2 = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, config2)
            .await
            .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer2.start().await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        // task should be consumed
        let task = collection
            .find_one(doc! {"key":"111"})
            .await
            .expect("failed to find")
            .expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let success_worker_state = &task.task_state.worker_states[0];
        assert!(success_worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_continuous() {
        let collection = init_collection_for_test("test_consume_continuous").await;
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        // init collection with some tasks
        task_producer
            .send_task("111", 0, None)
            .await
            .expect("failed to send task");
        task_producer
            .send_task("112", 0, None)
            .await
            .expect("failed to send task");
        let worker_id = "aaa";
        // start the consumer
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            TestConsumeFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        // wait for the consumer to complete all tasks
        tokio::time::sleep(Duration::from_secs(2)).await;
        // task should all be consumed
        assert!(collection
            .find_one(doc! {"task_state.worker_states.success_time":{"$eq":null}})
            .await
            .expect("failed to find")
            .is_none());
        // spawn more
        task_producer
            .send_task("113", 0, None)
            .await
            .expect("failed to send task");
        task_producer
            .send_task("114", 0, None)
            .await
            .expect("failed to send task");
        // wait for the consumer to complete all tasks
        tokio::time::sleep(Duration::from_secs(2)).await;
        // task should all be consumed
        assert!(collection
            .find_one(doc! {"task_state.worker_states.success_time":{"$eq":null}})
            .await
            .expect("failed to find")
            .is_none());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_no_update_tasks() {
        let collection = init_collection_for_test("test_consume_no_update_tasks").await;
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        // init collection with some tasks
        task_producer
            .send_task("111", 3, None)
            .await
            .expect("failed to send task");
        let worker_id = "aaa";
        // start the consumer
        let task_consumer = TaskConsumer::create(
            collection.clone(),
            TestConsumeWithTimeParamFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id).build(),
        )
        .await
        .expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        // wait for the consumer to occupy the task
        tokio::time::sleep(Duration::from_secs(1)).await;
        // task should be running
        assert!(collection
            .find_one(doc! {"task_state.worker_states.success_time":{"$eq":null}})
            .await
            .expect("failed to find")
            .is_some());
        // send task which will not update the running task
        let send_result = task_producer
            .send_task(
                "111",
                1,
                Some(SendTaskOption::builder().not_update_running(true).build()),
            )
            .await;
        assert!(send_result.is_err(), "send task not expected to success");
        let err = send_result.err().unwrap();
        assert!(
            matches!(err, MSchedulerError::DuplicatedTaskId),
            "send task should fail due to no find any matching task"
        );
        // wait for the consumer to complete the task
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            collection
                .find_one(doc! {"task_state.worker_states.success_time":{"$eq":null}})
                .await
                .expect("failed to find")
                .is_none(),
            "task should be consumed"
        );
        // send task which will not update the running task
        let send_result = task_producer.send_task("111", 1, None).await;
        assert!(send_result.is_ok(), "send task expected to success");
        assert!(
            send_result.unwrap().update_existing,
            "send task should update existing task"
        );
    }

    #[test_log::test(tokio::test)]
    pub async fn test_ping_task() {
        let collection = init_collection_for_test("test_ping_task").await;
        let task_producer =
            TaskProducer::create(collection.clone()).expect("failed to create producer");
        // init collection with some tasks
        let send_task_option = SendTaskOption::builder()
            .ping_interval_ms(1000_u32)
            .worker_timeout_ms(2500_u32)
            .build();
        task_producer
            .send_task("111", -6, Some(send_task_option))
            .await
            .expect("failed to send task");
        let worker_id01 = "aaa";
        // start the consumer
        let task_consumer1 = TaskConsumer::create(
            collection.clone(),
            TestConsumeWithTimeParamFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id01).build(),
        )
        .await
        .expect("failed to create consumer01");
        tokio::spawn({
            let task_consumer1 = task_consumer1.clone();
            async move { task_consumer1.start().await }
        });
        // wait for the consumer to occupy the task
        tokio::time::sleep(Duration::from_secs(1)).await;
        // create another consumer, this consumer should not acquire this task before it fails
        let worker_id02 = "bbb";
        let task_consumer2 = TaskConsumer::create(
            collection.clone(),
            TestConsumeWithTimeParamFunc {},
            TaskConsumerConfig::builder().worker_id(worker_id02).build(),
        )
        .await
        .expect("failed to create consumer02");
        tokio::spawn({
            let task_consumer2 = task_consumer2.clone();
            async move { task_consumer2.start().await }
        });
        // task should be running
        let task_time01 = collection
            .find_one(doc! {"task_state.worker_states.success_time":{"$eq":null}})
            .await
            .expect("failed to find")
            .expect("failed to get task");
        let first_ping_expire_time = task_time01
            .task_state
            .worker_states
            .get(0)
            .unwrap()
            .ping_expire_time;
        // wait for a while and check ping time again
        tokio::time::sleep(Duration::from_secs(2)).await;
        let task_time02 = collection
            .find_one(doc! {"task_state.worker_states.success_time":{"$eq":null}})
            .await
            .expect("failed to find")
            .expect("failed to get task");
        let second_ping_expire_time = task_time02
            .task_state
            .worker_states
            .get(0)
            .unwrap()
            .ping_expire_time;
        assert_ne!(
            first_ping_expire_time, second_ping_expire_time,
            "ping expire time not updated"
        );
        assert_eq!(
            task_consumer1.get_running_task_cnt(),
            1,
            "consumer 1 task should still be running"
        );
        assert_eq!(
            task_consumer2.get_running_task_cnt(),
            0,
            "consumer 2 should not occupy task before it finishes"
        );
        // wait for the task to fail and consumer2 should handle it
        let occupy_handler = spawn_check_handler(
            task_consumer2.clone(),
            |event| match event {
                ConsumerEvent::TaskOccupyResult { .. } => true,
                _ => false,
            },
            Duration::from_secs(10),
        );
        spawn_check_handler(
            task_consumer1.clone(),
            |event| match event {
                ConsumerEvent::TaskExecuteResult { .. } => true,
                _ => false,
            },
            Duration::from_secs(10),
        )
        .await
        .expect("failed to wait for execution result")
        .expect("failed to find result");
        occupy_handler
            .await
            .expect("failed to wait for consumer2 occupy result")
            .expect("failed to find consumer2 occupy");
        assert_eq!(
            task_consumer1.get_running_task_cnt(),
            0,
            "no task should be running in consumer 1"
        );
        assert_eq!(
            task_consumer2.get_running_task_cnt(),
            1,
            "one task should be running in consumer 2"
        );
    }
}
