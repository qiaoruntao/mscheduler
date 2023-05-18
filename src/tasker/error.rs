#[derive(Debug)]
pub enum MSchedulerError {
    AddTaskFailed,
    // mongo db returns an error that cannot get handled
    MongoDbError(mongodb::error::Error),
}

pub type MResult<T> = Result<T, MSchedulerError>;