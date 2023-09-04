pub trait TimesSeriesBlock {
    fn first_timestamp(&self) -> i64;
    fn last_timestamp(&self) -> i64;
    fn num_samples(&self) -> usize;

    fn remove_range(&mut self, start_ts: i64, end_ts: i64) -> usize;
}