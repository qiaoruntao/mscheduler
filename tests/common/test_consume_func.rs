use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use mscheduler::tasker::consumer::TaskConsumerFunc;
use mscheduler::tasker::error::MResult;
use mscheduler::tasker::error::MSchedulerError::ExecutionError;

pub struct TestConsumeFunc {}

#[derive(Default, TypedBuilder, Clone, Deserialize, Serialize)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TestConsumeParam {
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