use std::time::Duration;

use async_trait::async_trait;

use mscheduler::tasker::consumer::TaskConsumerFunc;
use mscheduler::tasker::error::MResult;
use mscheduler::tasker::error::MSchedulerError;

mod common;

struct TestConsumeFunc {}

#[async_trait]
impl TaskConsumerFunc<i32, i32> for TestConsumeFunc {
    async fn consumer(&self, params: Option<i32>) -> MResult<i32> {
        Ok(params.unwrap_or(0))
    }
}

struct TestStringConsumeFunc {}

#[async_trait]
impl TaskConsumerFunc<String, usize> for TestStringConsumeFunc {
    async fn consumer(&self, params: Option<String>) -> MResult<usize> {
        Ok(params.map(|v| v.len()).unwrap_or(0))
    }
}

struct TestConsumeFailFunc {}

#[async_trait]
impl TaskConsumerFunc<i32, i32> for TestConsumeFailFunc {
    async fn consumer(&self, _params: Option<i32>) -> MResult<i32> {
        let x = Box::new("".to_string());
        Err(MSchedulerError::ExecutionError(x))
    }
}

struct TestConsumeWithTimeParamFunc {}

#[async_trait]
impl TaskConsumerFunc<u64, u64> for TestConsumeWithTimeParamFunc {
    async fn consumer(&self, time: Option<u64>) -> MResult<u64> {
        if let Some(wait_time) = time {
            tokio::time::sleep(Duration::from_secs(wait_time)).await;
        }
        Ok(time.unwrap_or(0))
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use mongodb::bson::doc;

    use mscheduler::tasker::consumer::{TaskConsumer, TaskConsumerConfig};
    #[allow(unused_imports)]
    use mscheduler::tasker::error::MSchedulerError;
    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};

    use crate::{TestConsumeFailFunc, TestConsumeFunc, TestConsumeWithTimeParamFunc, TestStringConsumeFunc};
    use crate::common::get_collection;

    #[test_log::test(tokio::test)]
    pub async fn test_consume_task() {
        let collection = get_collection("test_consume_task").await;
        let consume_func = TestConsumeFunc {};
        let worker_id = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), consume_func, TaskConsumerConfig::builder().worker_id(worker_id).build()).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer.send_task("111", 1, None).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        assert_eq!(worker_state.worker_id, worker_id);
        assert!(worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_string_task() {
        let collection = get_collection("test_consume_string_task").await;
        let consume_func = TestStringConsumeFunc {};
        let worker_id = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), consume_func, TaskConsumerConfig::builder().worker_id(worker_id).build()).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer.send_task("111", "test".to_string(), None).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        assert_eq!(worker_state.worker_id, worker_id);
        assert!(worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_fail_task() {
        let collection = get_collection("test_consume_fail_task").await;
        let consume_func = TestConsumeFailFunc {};
        let worker_id = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), consume_func, TaskConsumerConfig::builder().worker_id(worker_id).build()).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        task_producer.send_task("111", 1, None).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let worker_state = task.task_state.worker_states.get(0).unwrap();
        assert_eq!(worker_state.worker_id, worker_id);
        assert_eq!(worker_state.success_time, None);
        assert!(worker_state.fail_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_multiple_consume_task() {
        let collection = get_collection("test_multiple_consume_task").await;
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        let worker_id2 = "bbb";
        let task_consumer2 = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id2).build()).await.expect("failed to create consumer");
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
        assert!(worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_partial_success_multiple_consume_task() {
        let collection = get_collection("test_partial_success_multiple_consume_task").await;
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), TestConsumeFailFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        let worker_id2 = "bbb";
        let task_consumer2 = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id2).build()).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        tokio::spawn(async move { task_consumer2.start().await });
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        let mut send_task_option = SendTaskOption::default();
        send_task_option.concurrency_cnt = 2;
        task_producer.send_task("111", 1, Some(send_task_option)).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(3)).await;
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 2);
        let success_worker_state = task.task_state.worker_states.iter().filter(|s| s.success_time.is_some()).next();
        assert!(success_worker_state.is_some());
        let success_worker_state = success_worker_state.unwrap();
        let fail_worker_state = task.task_state.worker_states.iter().filter(|s| s.fail_time.is_some()).next();
        assert!(fail_worker_state.is_some());
        let fail_worker_state = fail_worker_state.unwrap();
        assert!(success_worker_state.success_time.is_some());
        assert!(fail_worker_state.fail_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_task_worker_id() {
        let collection = get_collection("test_consume_task_worker_priority").await;
        let worker_id1 = "aaa";
        let task_consumer = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id1).build()).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });

        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        let mut send_task_option = SendTaskOption::default();
        send_task_option.concurrency_cnt = 1;
        send_task_option.min_worker_version = 1;
        task_producer.send_task("111", 1, Some(send_task_option)).await.expect("failed to send task");
        tokio::time::sleep(Duration::from_secs(1)).await;
        // task should not be consumed
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 0);
        // spawn a matched consumer
        let worker_id2 = "bbb";
        let config2 = TaskConsumerConfig::builder()
            .worker_version(1u32)
            .worker_id(worker_id2)
            .build();
        assert_eq!(config2.get_worker_id(), worker_id2);
        let task_consumer2 = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, config2).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer2.start().await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        // task should be consumed
        let task = collection.find_one(doc! {"key":"111"}, None).await.expect("failed to find").expect("no task found");
        assert_eq!(task.task_state.worker_states.len(), 1);
        let success_worker_state = &task.task_state.worker_states[0];
        assert!(success_worker_state.success_time.is_some());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_continuous() {
        let collection = get_collection("test_consume_continuous").await;
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        // init collection with some tasks
        task_producer.send_task("111", 0, None).await.expect("failed to send task");
        task_producer.send_task("112", 0, None).await.expect("failed to send task");
        let worker_id = "aaa";
        // start the consumer
        let task_consumer = TaskConsumer::create(collection.clone(), TestConsumeFunc {}, TaskConsumerConfig::builder().worker_id(worker_id).build()).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        // wait for the consumer to complete all tasks
        tokio::time::sleep(Duration::from_secs(2)).await;
        // task should all be consumed
        assert!(collection.find_one(doc! {"task_state.worker_states.success_time":{"$exists":false}}, None).await.expect("failed to find").is_none());
        // spawn more
        task_producer.send_task("113", 0, None).await.expect("failed to send task");
        task_producer.send_task("114", 0, None).await.expect("failed to send task");
        // wait for the consumer to complete all tasks
        tokio::time::sleep(Duration::from_secs(2)).await;
        // task should all be consumed
        assert!(collection.find_one(doc! {"task_state.worker_states.success_time":{"$exists":false}}, None).await.expect("failed to find").is_none());
    }

    #[test_log::test(tokio::test)]
    pub async fn test_consume_no_update_tasks() {
        let collection = get_collection("test_consume_no_update_tasks").await;
        let task_producer = TaskProducer::create(collection.clone()).expect("failed to create producer");
        // init collection with some tasks
        task_producer.send_task("111", 3, None).await.expect("failed to send task");
        let worker_id = "aaa";
        // start the consumer
        let task_consumer = TaskConsumer::create(collection.clone(), TestConsumeWithTimeParamFunc {}, TaskConsumerConfig::builder().worker_id(worker_id).build()).await.expect("failed to create consumer");
        tokio::spawn(async move { task_consumer.start().await });
        // wait for the consumer to occupy the task
        tokio::time::sleep(Duration::from_secs(1)).await;
        // task should be running
        assert!(collection.find_one(doc! {"task_state.worker_states.success_time":{"$exists":false}}, None).await.expect("failed to find").is_some());
        // send task which will not update the running task
        let send_result = task_producer.send_task("111", 1, Some(SendTaskOption::builder().not_update_running(true).build())).await;
        assert!(send_result.is_err(), "send task not expected to success");
        let err = send_result.err().unwrap();
        assert!(matches!(err, MSchedulerError::DuplicatedTaskId), "send task should fail due to no find any matching task");
        // wait for the consumer to complete the task
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(collection.find_one(doc! {"task_state.worker_states.success_time":{"$exists":false}}, None).await.expect("failed to find").is_none(), "task should be consumed");
        // send task which will not update the running task
        let send_result = task_producer.send_task("111", 1, None).await;
        assert!(send_result.is_ok(), "send task expected to success");
        assert!(send_result.unwrap().update_existing, "send task should update existing task");
    }
}