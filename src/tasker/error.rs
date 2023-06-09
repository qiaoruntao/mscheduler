use std::sync::Arc;
use strum::Display;

#[derive(Debug, Display)]
pub enum MSchedulerError {
    AddTaskFailed,
    NoTaskMatched,
    // mongo db returns an error that cannot get handled
    MongoDbError(Arc<mongodb::error::Error>),
}

pub type MResult<T> = Result<T, MSchedulerError>;