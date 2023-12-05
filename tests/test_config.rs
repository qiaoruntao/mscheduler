#[cfg(test)]
mod test {
    use mscheduler::tasker::consumer::TaskConsumerConfig;

    #[test]
    pub fn test_config() {
        let config = TaskConsumerConfig::builder().worker_id("111").build();
        assert_eq!(config.worker_id, "111");
    }
}