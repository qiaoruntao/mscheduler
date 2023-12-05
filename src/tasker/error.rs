use std::fmt::Debug;
use std::sync::Arc;
use strum::Display;

#[derive(Debug, Display)]
pub enum MSchedulerError {
    // failed to send task
    AddTaskFailed,
    // cannot find matched task
    NoTaskMatched,
    // duplicated task can be caused by various reasons
    DuplicatedTaskId,
    // unknown error
    UnknownError,
    // panic during task execution
    PanicError,
    // shutting down
    ConsumerShutdownError,
    // panic during task execution
    TaskCancelled,
    // task execution logic send back some error
    ExecutionError(Box<dyn Send + Sync + Debug + 'static>),
    // mongo db returns an error that cannot get handled
    MongoDbError(Arc<mongodb::error::Error>),
}

pub type MResult<T> = Result<T, MSchedulerError>;