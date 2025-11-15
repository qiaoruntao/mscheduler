mod common;

#[cfg(test)]
mod test {
    use mongodb::bson::{doc, DateTime};

    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};
    use mscheduler::tasker::task::TaskWorkerRunningInfo;
    use mscheduler::tasker::task_common::ensure_index;

    use crate::common::test::init_collection_for_test;

    #[tokio::test]
    async fn test_send_new_task() {
        let collection = init_collection_for_test("test_send_new_task").await;
        collection
            .delete_many(doc! {})
            .await
            .expect("failed to clear collection");
        ensure_index(&collection).await;

        let task_producer = TaskProducer::<_, i32>::create(collection.clone())
            .expect("failed to generate producer");
        let random = DateTime::now().timestamp_millis() % 1000;
        let key = "111";
        let send_task_result = task_producer
            .send_task(key, random as i32, None)
            .await
            .expect("failed to send new task");
        let task = collection
            .find_one(doc! {"key":key})
            .await
            .expect("failed to find new task in db")
            .expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random as i32);
        assert_eq!(task.key, key);
        assert!(!send_task_result.update_existing);
        assert!(send_task_result.insert_new);
    }

    #[tokio::test]
    async fn test_send_duplicate_task() {
        let collection = init_collection_for_test("test_send_duplicate_task").await;
        collection
            .delete_many(doc! {})
            .await
            .expect("failed to clear collection");
        ensure_index(&collection).await;

        let task_producer = TaskProducer::<_, i32>::create(collection.clone())
            .expect("failed to generate producer");
        let random = (DateTime::now().timestamp_millis() % 1000) as i32;
        let key = "111";
        // insert a new task
        let send_task_result = task_producer
            .send_task(key, random, None)
            .await
            .expect("failed to send new task");
        let task = collection
            .find_one(doc! {"key":key})
            .await
            .expect("failed to find new task in db")
            .expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random);
        assert_eq!(task.key, key);
        assert!(!send_task_result.update_existing);
        assert!(send_task_result.insert_new);
        // reinsert task with a different parameter, but not specify to update parameter
        task_producer
            .send_task(key, random + 1, None)
            .await
            .expect("failed to send new task");
        let task = collection
            .find_one(doc! {"key":key})
            .await
            .expect("failed to find new task in db")
            .expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random);
        assert_eq!(task.key, key);
        let send_task_option = SendTaskOption::builder()
            .update_existing_params(true)
            .build();
        // reinsert task with a different parameter, but specific to update parameter
        task_producer
            .send_task(key, random + 1, Some(send_task_option))
            .await
            .expect("failed to send new task");
        let task = collection
            .find_one(doc! {"key":key})
            .await
            .expect("failed to find new task in db")
            .expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random + 1);
        assert_eq!(task.key, key);
        // reinsert task with a different parameter, but specific to update parameter
        let run_time = DateTime::parse_rfc3339_str("2030-04-12T23:20:50.52Z").unwrap();
        let send_task_option = SendTaskOption::builder().run_time(Some(run_time)).build();
        // reinsert task with a different run time
        let send_task_result = task_producer
            .send_task(key, random + 1, Some(send_task_option))
            .await
            .expect("failed to send new task");
        assert!(send_task_result.update_existing);
        assert!(!send_task_result.insert_new);
        let task = collection
            .find_one(doc! {"key":key})
            .await
            .expect("failed to find new task in db")
            .expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.task_state.start_time, run_time);
        assert_eq!(task.key, key);
    }

    #[tokio::test]
    async fn test_send_task_clean_failed_worker_states() {
        let collection =
            init_collection_for_test("test_send_task_clean_failed_worker_states").await;
        collection
            .delete_many(doc! {})
            .await
            .expect("failed to clear collection");
        ensure_index(&collection).await;

        let task_producer = TaskProducer::<_, i32>::create(collection.clone())
            .expect("failed to generate producer");
        let key = "clean_failed_key";
        task_producer
            .send_task(key, 1_i32, None)
            .await
            .expect("failed to send base task");

        let failed_worker_state = TaskWorkerRunningInfo::<i32> {
            running_id: Some("fail".to_string()),
            worker_id: "worker_1".to_string(),
            ping_expire_time: Some(DateTime::now()),
            success_time: None,
            fail_time: Some(DateTime::now()),
            fail_reason: Some("expected failure".to_string()),
            returns: None,
        };
        let success_worker_state = TaskWorkerRunningInfo::<i32> {
            running_id: Some("success".to_string()),
            worker_id: "worker_2".to_string(),
            ping_expire_time: Some(DateTime::now()),
            success_time: Some(DateTime::now()),
            fail_time: None,
            fail_reason: None,
            returns: None,
        };
        let worker_states = vec![failed_worker_state, success_worker_state];
        collection
            .update_one(
                doc! {"key":key},
                doc! {"$set":{
                    "task_state.worker_states": mongodb::bson::to_bson(&worker_states).unwrap()
                }},
            )
            .await
            .expect("failed to set worker states");

        task_producer
            .send_task(key, 2_i32, None)
            .await
            .expect("failed to resend task without clean option");
        let task = collection
            .find_one(doc! {"key":key})
            .await
            .expect("failed to fetch task in db")
            .expect("task should exist");
        assert_eq!(task.task_state.worker_states.len(), 2);
        assert_eq!(
            task.task_state
                .worker_states
                .iter()
                .filter(|state| state.fail_time.is_some())
                .count(),
            1
        );

        let send_task_option = SendTaskOption::builder().clean_failed(true).build();
        task_producer
            .send_task(key, 3_i32, Some(send_task_option))
            .await
            .expect("failed to resend task with clean_failed option");
        let task = collection
            .find_one(doc! {"key":key})
            .await
            .expect("failed to fetch task after clean")
            .expect("task should exist");
        assert_eq!(task.task_state.worker_states.len(), 1);
        assert!(task
            .task_state
            .worker_states
            .iter()
            .all(|state| state.fail_time.is_none()));
    }
}
