mod common;

#[cfg(test)]
mod test {
    use mongodb::bson::{DateTime, doc};

    use mscheduler::tasker::producer::{SendTaskOption, TaskProducer};
    use mscheduler::tasker::task_common::ensure_index;

    use crate::common::get_collection;

    #[tokio::test]
    async fn test_send_new_task() {
        let collection = get_collection("test_send_new_task").await;
        collection.delete_many(doc! {}, None).await.expect("failed to clear collection");
        ensure_index(&collection).await;

        let task_producer = TaskProducer::create(collection.clone()).expect("failed to generate producer");
        let random = DateTime::now().timestamp_millis() % 1000;
        let key = "111";
        task_producer.send_task(key, random as i32, None).await.expect("failed to send new task");
        let task = collection.find_one(doc! {"key":key}, None).await.expect("failed to find new task in db").expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random as i32);
        assert_eq!(task.key, key);
    }

    #[tokio::test]
    async fn test_send_duplicate_task() {
        let collection = get_collection("test_send_duplicate_task").await;
        collection.delete_many(doc! {}, None).await.expect("failed to clear collection");
        ensure_index(&collection).await;

        let task_producer = TaskProducer::create(collection.clone()).expect("failed to generate producer");
        let random = (DateTime::now().timestamp_millis() % 1000) as i32;
        let key = "111";
        // insert a new task
        task_producer.send_task(key, random, None).await.expect("failed to send new task");
        let task = collection.find_one(doc! {"key":key}, None).await.expect("failed to find new task in db").expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random);
        assert_eq!(task.key, key);
        // reinsert task with a different parameter, but no specific to update parameter
        task_producer.send_task(key, random + 1, None).await.expect("failed to send new task");
        let task = collection.find_one(doc! {"key":key}, None).await.expect("failed to find new task in db").expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random);
        assert_eq!(task.key, key);
        let mut send_task_option = SendTaskOption::default();
        send_task_option.update_existing_params = true;
        // reinsert task with a different parameter, but specific to update parameter
        task_producer.send_task(key, random + 1, Some(send_task_option)).await.expect("failed to send new task");
        let task = collection.find_one(doc! {"key":key}, None).await.expect("failed to find new task in db").expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.params.unwrap(), random + 1);
        assert_eq!(task.key, key);
        // reinsert task with a different parameter, but specific to update parameter
        let run_time = DateTime::parse_rfc3339_str("2030-04-12T23:20:50.52Z").unwrap();
        let mut send_task_option = SendTaskOption::default();
        send_task_option.run_time = Some(run_time);
        // reinsert task with a different run time
        task_producer.send_task(key, random + 1, Some(send_task_option)).await.expect("failed to send new task");
        let task = collection.find_one(doc! {"key":key}, None).await.expect("failed to find new task in db").expect("no task returns");
        assert!(task.params.is_some());
        assert_eq!(task.task_state.start_time, run_time);
        assert_eq!(task.key, key);
    }
}