use mongodb::bson::{DateTime, doc, to_bson, to_document};
use mongodb::Collection;
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::UpdateOptions;
use serde::Serialize;
use tracing::{error, trace};
use typed_builder::TypedBuilder;

use crate::tasker::error::{MResult, MSchedulerError};
use crate::tasker::task::{Task, TaskOption};

pub struct TaskProducer<T, K> {
    task_collection: Collection<Task<T, K>>,
}

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SendTaskOption {
    // whether to update existing task params
    #[builder(default = false)]
    pub update_existing_params: bool,
    // specific a custom task start time
    #[builder(default = None)]
    pub run_time: Option<DateTime>,
    // how many concurrency workers are allowed
    #[builder(default = 1)]
    pub concurrency_cnt: u32,
    #[builder(default = 0)]
    pub min_worker_version: u32,
    // do not find and do anything to a running task
    #[builder(default = true)]
    pub not_update_running: bool,
    #[builder(default = 30_000)]
    pub ping_interval_ms: u32,
    // clean up existing task's success worker states
    // pub clean_success: bool,
    // clean up existing task's failed worker states
    // pub clean_failed: bool,
    // TODO: more options in task_option
    #[builder(default = 60_000)]
    worker_timeout_ms: u32,
}

pub struct SendTaskResult {
    pub insert_new: bool,
    pub update_existing: bool,
}

impl<T: Serialize, K: Serialize> TaskProducer<T, K> {
    pub fn create(collection: Collection<Task<T, K>>) -> MResult<TaskProducer<T, K>> {
        Ok(TaskProducer {
            task_collection: collection,
        })
    }

    /// send a task
    pub async fn send_task(&self, key: impl AsRef<str>, params: T, option: Option<SendTaskOption>) -> MResult<SendTaskResult> {
        let send_option = option.unwrap_or_else(|| SendTaskOption::builder().build());
        let mut query = doc! { "key": key.as_ref()};
        if send_option.not_update_running {
            let doc = doc! {
                "$all":[{
                    "$or":[
                        {"success_time":{"$eq": null}},
                        {"fail_time":{"$eq": null}},
                    ]
                }]
            };
            // find a task that all worker is success or failed
            query.insert("task_state.worker_states", doc);
        }


        let now = DateTime::now();
        let start_time = send_option.run_time.clone().unwrap_or(now);
        let task_option = TaskOption {
            priority: 0,
            concurrent_worker_cnt: send_option.concurrency_cnt,
            ping_interval_ms: send_option.ping_interval_ms,
            worker_timeout_ms: send_option.worker_timeout_ms,
            min_worker_version: send_option.min_worker_version,
            specific_worker_ids: vec![],
            max_unexpected_retries: 3,
            unexpected_retry_delay_ms: 10_000,
        };
        let mut update_part = doc! {
            "$setOnInsert": doc! {
                "key":key.as_ref(),
                "task_state.create_time":now,
                "task_state.start_time":start_time,
                "task_state.worker_states":[],
                "task_option":to_document(&task_option).unwrap(),
            },
        };
        let mut updates = vec![];
        // decide where to put params, params can appear in either $setOnInsert or $set
        if send_option.update_existing_params {
            updates.push(("params", to_bson(&params).unwrap()));
        } else {
            let set_on_insert_part = update_part.get_mut("$setOnInsert").unwrap();
            let set_on_insert_doc = set_on_insert_part.as_document_mut().unwrap();
            set_on_insert_doc.insert("params", to_bson(&params).unwrap());
        }
        // update task run time if specific
        if let Some(_) = send_option.run_time {
            let set_on_insert_part = update_part.get_mut("$setOnInsert").unwrap();
            let set_on_insert_doc = set_on_insert_part.as_document_mut().unwrap();
            let task_state_part = set_on_insert_doc.remove("task_state.start_time").unwrap();
            updates.push(("task_state.start_time", task_state_part));
        }
        // convert these updates to actual set clause
        if !updates.is_empty() {
            let mut document = doc! {};
            for update in updates {
                document.insert(update.0, update.1);
            }
            update_part.insert("$set", document);
        }
        let mut update_options = UpdateOptions::default();
        update_options.upsert = Some(true);

        match self.task_collection.update_one(query, update_part, Some(update_options)).await {
            Ok(v) => {
                if v.upserted_id.is_some() {
                    trace!("send task and new task is created key={}",key.as_ref());
                    Ok(SendTaskResult { insert_new: true, update_existing: false })
                } else if v.matched_count == 1 {
                    // TODO: check modified count
                    trace!("send task ignored key={}",key.as_ref());
                    Ok(SendTaskResult { insert_new: false, update_existing: true })
                } else if v.matched_count == 0 {
                    trace!("send task is ignored key={}",key.as_ref());
                    Err(MSchedulerError::NoTaskMatched)
                } else {
                    error!("send task is failed key={}",key.as_ref());
                    Err(MSchedulerError::AddTaskFailed)
                }
            }
            Err(e) => {
                match e.kind.as_ref() {
                    ErrorKind::Write(WriteFailure::WriteError(write_error)) => {
                        if write_error.code == 11000 {
                            trace!("task inserted failed, duplicated key");
                            Err(MSchedulerError::DuplicatedTaskId)
                        } else {
                            error!("failed to send task {}",e);
                            Err(MSchedulerError::MongoDbError(e.into()))
                        }
                    }
                    _ => {
                        error!("unknown mongodb error occurred during insert {:?}", &e);
                        Err(MSchedulerError::MongoDbError(e.into()))
                    }
                }
            }
        }
    }
}