use std::collections::BinaryHeap;
use std::fmt::Display;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::ts::data_point::DataPoint;
use crate::utils::custom_serde::rwlock_serde;
use crate::utils::error::TsldbError;
use crate::utils::range::is_overlap;
use crate::utils::sync::RwLock;

use super::constants::BLOCK_SIZE_FOR_TIME_SERIES;
use super::time_series_block::TimeSeriesBlock;
use super::time_series_block_compressed::TimeSeriesBlockCompressed;

#[derive(Debug, Deserialize, Serialize)]
pub enum DuplicatePolicy {
  /// ignore any newly reported value and reply with an error
  Block,
  /// ignore any newly reported value
  First,
  /// overwrite the existing value with the new value
  Last,
  /// only override if the value is lower than the existing value
  Min,
  /// only override if the value is higher than the existing value
  Max,
  /// append the new value to the existing value
  Sum
}

impl Display for DuplicatePolicy {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      DuplicatePolicy::Block => write!(f, "block"),
      DuplicatePolicy::First => write!(f, "first"),
      DuplicatePolicy::Last => write!(f, "last"),
      DuplicatePolicy::Min => write!(f, "min"),
      DuplicatePolicy::Max => write!(f, "max"),
      DuplicatePolicy::Sum => write!(f, "sum"),
    }
  }
}

impl From<&str> for DuplicatePolicy {
  fn from(s: &str) -> Self {
    match s.to_ascii_lowercase().as_str() {
      "block" => DuplicatePolicy::Block,
      "first" => DuplicatePolicy::First,
      "last" => DuplicatePolicy::Last,
      "min" => DuplicatePolicy::Min,
      "max" => DuplicatePolicy::Max,
      "sum" => DuplicatePolicy::Sum,
      _ => panic!("invalid duplicate policy: {}", s),
    }
  }
}

/// Separator between the label name and label value to create a label term. For example,
/// if the label name is 'method' and the value is 'GET',and the LABEL_SEPARATOR is '~',
/// in the labels map this will be stored as 'method~GET'.
const LABEL_SEPARATOR: &str = "~";

/// The label for the metric name when stored in the time series. For example, if the METRIC_NAME_PREFIX
/// is '__name__', the LABEL_SEPARATOR is '~', and the matrix name is 'request_count', in the labels map,
/// this will be stored as '__name__~request_count'.
const METRIC_NAME_PREFIX: &str = "__name__";

/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// data points. All but the last block are compressed. In order to quickly get to the right block, a vector of
/// initial values in each block is also stored (also called 'skip pointer' in literature).
#[derive(Debug, Deserialize, Serialize)]
pub struct TimeSeries {
  id: u64,
  
  retention: Duration,
  
  duplicate_policy: DuplicatePolicy,

  chunk_size_bytes: usize,

  // dedupe_interval
  
  /// A list of compressed time series blocks.
  #[serde(with = "rwlock_serde")]
  compressed_blocks: RwLock<Vec<TimeSeriesBlockCompressed>>,

  /// We only compress blocks that have 128 integers. The last block which
  /// may have <=127 integers is stored in uncompressed form.
  #[serde(with = "rwlock_serde")]
  last_block: RwLock<TimeSeriesBlock>,

  /// The initial timestamps in the time series blocks. The length of the initial
  /// values will 1 plus length of the 'compressed_blocks'.
  /// (The additional 1 is to account for the uncompressed 'last' block)
  #[serde(with = "rwlock_serde")]
  initial_times: RwLock<Vec<u64>>,

  pub total_samples: u64,

  pub first_timestamp: i64,

  pub last_timestamp: i64,

  pub last_value: f64,
}

impl TimeSeries {
  /// Create a new empty time series.
  pub fn new() -> Self {
    TimeSeries {
      id: 0,
      retention: Default::default(),
      duplicate_policy: DuplicatePolicy::Block,
      chunk_size_bytes: 4096,
      compressed_blocks: RwLock::new(Vec::new()),
      last_block: RwLock::new(TimeSeriesBlock::new()),
      initial_times: RwLock::new(Vec::new()),
      total_samples: 0,
      first_timestamp: 0,
      last_timestamp: 0,
      last_value: f64::NAN,
    }
  }

  /// Append the given time and value to the time series.
  pub fn append(&mut self, time: u64, value: f64) {
    // Grab all the locks - as we may need to update any of these. In any scenario where all the locks need to captured,
    // the consistent sequence of capturing is important as otherwise it can lead to deadlock.
    let mut compressed_blocks_lock = self.compressed_blocks.write().unwrap();
    let mut last_block_lock = self.last_block.write().unwrap();
    let mut initial_times_lock = self.initial_times.write().unwrap();

    let mut is_initial = false;
    if last_block_lock.is_empty() {
      // First insertion in this time series.
      is_initial = true;
      self.first_timestamp = time as i64;
      self.last_timestamp = time as i64;
      self.last_value = value;
    }

    // Try to append the time+value to the last block.
    let ret_val = last_block_lock.append(time, value);

    if ret_val.is_err()
      && ret_val.err().unwrap() == TsldbError::CapacityFull(BLOCK_SIZE_FOR_TIME_SERIES)
    {
      // The last block is full. So, compress it and append it time_series_block_compressed.
      let tsbc: TimeSeriesBlockCompressed =
        TimeSeriesBlockCompressed::try_from(&*last_block_lock).unwrap();
      compressed_blocks_lock.push(tsbc);

      // Create a new last block and append the time+value to it.
      *last_block_lock = TimeSeriesBlock::new();
      last_block_lock.append(time, value).unwrap();

      // We created a new block and pushed initial value - so set is_initial to true.
      is_initial = true;
    }

    // If the is_initial flag is set, append the time to initial times vector.
    if is_initial {
      initial_times_lock.push(time);
    }
    if time < self.first_timestamp as u64 {
      self.first_timestamp = time as i64;
    }

    if time > self.last_timestamp as u64 {
      self.last_timestamp = time as i64;
      self.last_value = value;
    }
  }

  /// Get the time series between give start and end time (both inclusive).
  pub fn get_time_series(&self, range_start_time: u64, range_end_time: u64) -> Vec<DataPoint> {
    // While each TimeSeriesBlock as well as TimeSeriesBlockCompressed has data points sorted by time, in a
    // multithreaded environment, they might not be sorted across blocks. Hence, we collect all the datapoints in a heap,
    // and return a vector created from the heap, so that the return value is a sorted vector of data points.

    let mut retval: BinaryHeap<DataPoint> = BinaryHeap::new();

    let initial_times = self.initial_times.read().unwrap();
    let compressed_blocks = self.compressed_blocks.read().unwrap();
    let last_block = self.last_block.read().unwrap();

    // Get overlapping data points from the compressed blocks.
    if initial_times.len() > 1 {
      for i in 0..initial_times.len() - 1 {
        let block_start = *initial_times.get(i).unwrap();

        // The maximum block end time would be one less than the start time of the next block.
        let block_end = initial_times.get(i + 1).unwrap() - 1;

        if is_overlap(block_start, block_end, range_start_time, range_end_time) {
          let compressed_block = compressed_blocks.get(i).unwrap();
          let block = TimeSeriesBlock::try_from(compressed_block).unwrap();
          let data_points_in_range =
            block.get_data_points_in_range(range_start_time, range_end_time);
          for dp in data_points_in_range {
            retval.push(dp);
          }
        }
      }
    }

    // Get overlapping data points from the last block.
    if initial_times.last().is_some() {
      let data_points_in_range =
        last_block.get_data_points_in_range(range_start_time, range_end_time);
      for dp in data_points_in_range {
        retval.push(dp);
      }
    }

    retval.into_sorted_vec()
  }

  /// Get the label term that is used for given metric name.
  pub fn get_label_for_metric_name(metric_name: &str) -> String {
    format!("{METRIC_NAME_PREFIX}{LABEL_SEPARATOR}{metric_name}")
  }

  /// Get the label term used for given label name and label value.
  pub fn get_label<'a>(label_name: &'a str, label_value: &'a str) -> String {
    format!("{label_name}{LABEL_SEPARATOR}{label_value}")
  }

  #[cfg(test)]
  /// Get the last block, wrapped in RwLock.
  pub fn get_last_block(&self) -> &RwLock<TimeSeriesBlock> {
    &self.last_block
  }

  #[cfg(test)]
  /// Get the vector of compressed blocks, wrapped in RwLock.
  pub fn get_compressed_blocks(&self) -> &RwLock<Vec<TimeSeriesBlockCompressed>> {
    &self.compressed_blocks
  }

  #[cfg(test)]
  /// Get the initial times, wrapped in RwLock.
  pub fn get_initial_times(&self) -> &RwLock<Vec<u64>> {
    &self.initial_times
  }

  pub fn delete_range(&mut self, start_ts: i64, end_ts: i64) {

  }

  pub fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
    self.last_timestamp >= start_ts && self.first_timestamp <= end_ts
  }

  pub fn trim(&mut self, start_ts: i64, end_ts: i64) {
    if self.retention.as_millis() == 0 {
      return;
    }
    let compressed_blocks = self.compressed_blocks.read().unwrap();
    let last_block = self.last_block.read().unwrap();

    let min_timestamp = self.get_min_timestamp();
    let mut count = 0;
    let mut deleted_count = 0;

    for chunk in compressed_blocks.iter()
        .take_while(|&block| block.first_timestamp().unwrap_or(0) < min_timestamp) {
      count += 1;
      deleted_count += chunk.len();
    }

    if count > 0 {
        let _ = self.compressed_blocks.write().unwrap().drain(0..count);
    }
    // todo: trim partial compressed chunks
    if last_block.first_timestamp().unwrap_or(0) < min_timestamp {
      let _ = self.last_block.write().unwrap().remove_range(start_ts as u64, end_ts as u64);
    }
    // trim last block
    deleted_count += last_block.trim_to(min_timestamp + 1, end_ts);

    self.total_samples -= deleted_count as u64;

  }

  pub fn remove_range(&mut self, start_ts: i64, end_ts: i64) -> usize {

    let mut deleted_samples = 0;
    // todo: tinyvec
    let mut indexes_to_delete = Vec::new();

    let mut compressed_blocks = self.compressed_blocks.read().unwrap();
    let last_block = self.last_block.read().unwrap();

    for (idx, chunk) in compressed_blocks.iter_mut().enumerate() {
      let chunk_first_ts = chunk.first_timestamp();
      let chunk_last_ts = chunk.last_timestamp();

      // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
      // larger than end_ts
      if chunk.is_empty() || chunk.first_timestamp() > end_ts {
        // Having empty chunk means the series is empty
        break;
      }

      if chunk.last_timestamp() < start_ts {
        continue;
      }

      let is_only_chunk =
          (chunk.num_samples() + deleted_samples) == self.total_samples;

      // Should we delete the entire chunk?
      let ts_del_condition =
          (chunk_first_ts >= start_ts && chunk_last_ts <= end_ts) &&
              (!is_only_chunk); // We assume at least one allocated chunk in the series

      if !ts_del_condition {
        deleted_samples += chunk.remove_range(start_ts, end_ts)?;
        continue;
      }

      let is_last_chunk_deleted = chunk == self.last_block;
      deleted_samples += chunk.num_samples();
      indexes_to_delete.push(idx);
    }

    self.total_samples -= deleted_samples;

    for idx in indexes_to_delete.iter().rev() {
      let _ = compressed_blocks.remove(*idx);
    }

    // Check if last timestamp deleted
    if end_ts >= self.last_timestamp && start_ts <= self.last_timestamp {
      let current_chunk = self.last_chunk;
      match self.chunks.iter().last() {
        Some(chunk) => {
          self.last_timestamp = chunk.last_timestamp();
          self.last_value = chunk.last_value();
        }
        None => {
          self.last_timestamp = 0;
          self.last_value = f64::NAN;
        }
      }
    }

    // CompactionDelRange(series, start_ts, end_ts, last_ts_before_deletion);

    return deleted_samples
  }

  fn get_min_timestamp(&self) -> i64 {
    let last_ts = self.last_timestamp;
    let retention_millis = self.retention.as_millis() as i64;
    if self.last_timestamp > retention_millis {
      self.last_timestamp - retention_millis
    } else {
      0
    }
  }
}

impl Default for TimeSeries {
  fn default() -> Self {
    Self::new()
  }
}

impl PartialEq for TimeSeries {
  fn eq(&self, other: &Self) -> bool {
    let compressed_blocks_lock = self.compressed_blocks.read().unwrap();
    let other_compressed_blocks_lock = other.compressed_blocks.read().unwrap();
    let last_block_lock = self.last_block.read().unwrap();
    let other_last_block_lock = other.last_block.read().unwrap();
    let initial_times_lock = self.initial_times.read().unwrap();
    let other_initial_times_lock = other.initial_times.read().unwrap();

    *compressed_blocks_lock == *other_compressed_blocks_lock
      && *initial_times_lock == *other_initial_times_lock
      && *last_block_lock == *other_last_block_lock
  }
}

impl Eq for TimeSeries {}

#[cfg(test)]
mod tests {
  use std::sync::Arc;
  use std::thread;

  use rand::Rng;

  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  fn test_new() {
    // Check that the time series implements sync.
    is_sync::<TimeSeries>();

    // Check that a new time series is empty.
    let ts = TimeSeries::new();
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);
    assert_eq!(ts.last_block.read().unwrap().len(), 0);
    assert_eq!(ts.initial_times.read().unwrap().len(), 0);
  }

  #[test]
  fn test_default() {
    let ts = TimeSeries::default();

    // Check that a default time series is empty.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);
    assert_eq!(ts.last_block.read().unwrap().len(), 0);
    assert_eq!(ts.initial_times.read().unwrap().len(), 0);
  }

  #[test]
  fn test_one_entry() {
    let ts = TimeSeries::new();
    ts.append(100, 200.0);

    // The entry should get apppended only to 'last' block.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);

    assert_eq!(ts.last_block.read().unwrap().len(), 1);
    let last_block_lock = ts.last_block.read().unwrap();
    let time_series_data_points = &*last_block_lock
      .get_time_series_data_points()
      .read()
      .unwrap();
    let data_point = time_series_data_points.get(0).unwrap();
    assert_eq!(data_point.get_time(), 100);
    assert_eq!(data_point.get_value(), 200.0);

    assert_eq!(ts.initial_times.read().unwrap().len(), 1);
    assert_eq!(ts.initial_times.read().unwrap().get(0).unwrap(), &100);
  }

  #[test]
  fn test_block_size_entries() {
    let ts = TimeSeries::new();
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      ts.append(i as u64, i as f64);
    }

    // All the entries will go to 'last', as we have pushed exactly BLOCK_SIZE_FOR_TIME_SERIES entries.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);
    assert_eq!(
      ts.last_block.read().unwrap().len(),
      BLOCK_SIZE_FOR_TIME_SERIES
    );
    assert_eq!(ts.initial_times.read().unwrap().len(), 1);
    assert_eq!(ts.initial_times.read().unwrap().get(0).unwrap(), &0);

    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      let last_block_lock = ts.last_block.read().unwrap();
      let time_series_data_points = &*last_block_lock
        .get_time_series_data_points()
        .read()
        .unwrap();
      let data_point = time_series_data_points.get(i).unwrap();
      assert_eq!(data_point.get_time(), i as u64);
      assert_eq!(data_point.get_value(), i as f64);
    }
  }

  #[test]
  fn test_block_size_plus_one_entries() {
    let ts = TimeSeries::new();

    // Append block_size+1 entries, so that two blocks are created.
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES + 1 {
      ts.append(i as u64, i as f64);
    }

    // We should have 1 compressed block with 128 entries, and a last block with 1 entry.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 1);
    assert_eq!(ts.last_block.read().unwrap().len(), 1);

    // There should be 2 initial_times per the two blocks, with start times 0 and
    // BLOCK_SIZE_FOR_TIME_SERIES
    assert_eq!(ts.initial_times.read().unwrap().len(), 2);
    assert_eq!(ts.initial_times.read().unwrap().get(0).unwrap(), &0);
    assert_eq!(
      ts.initial_times.read().unwrap().get(1).unwrap(),
      &(BLOCK_SIZE_FOR_TIME_SERIES as u64)
    );

    let uncompressed =
      TimeSeriesBlock::try_from(ts.compressed_blocks.read().unwrap().get(0).unwrap()).unwrap();
    assert_eq!(uncompressed.len(), BLOCK_SIZE_FOR_TIME_SERIES);
    let data_points_lock = uncompressed.get_time_series_data_points().read().unwrap();
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      let data_point = data_points_lock.get(i).unwrap();
      assert_eq!(data_point.get_time(), i as u64);
      assert_eq!(data_point.get_value(), i as f64);
    }
  }

  #[test]
  fn test_data_points_in_range() {
    let num_blocks = 4;
    let ts = TimeSeries::new();
    let num_data_points = num_blocks * BLOCK_SIZE_FOR_TIME_SERIES as u64;
    for i in 0..num_data_points {
      ts.append(i as u64, i as f64);
    }

    assert_eq!(
      ts.get_time_series(0, num_data_points - 1).len() as u64,
      num_data_points
    );
    assert_eq!(
      ts.get_time_series(0, num_data_points + 1000).len() as u64,
      num_data_points
    );

    assert_eq!(
      ts.get_time_series(0, BLOCK_SIZE_FOR_TIME_SERIES as u64)
        .len(),
      BLOCK_SIZE_FOR_TIME_SERIES + 1
    );

    assert_eq!(
      ts.get_time_series(
        BLOCK_SIZE_FOR_TIME_SERIES as u64,
        BLOCK_SIZE_FOR_TIME_SERIES as u64 + 10
      )
      .len(),
      11
    );
  }

  #[test]
  fn test_concurrent_append() {
    let num_blocks: usize = 10;
    let num_threads = 16;
    let num_data_points_per_thread = num_blocks * BLOCK_SIZE_FOR_TIME_SERIES / num_threads;
    let ts = Arc::new(TimeSeries::new());

    let mut handles = Vec::new();
    let expected = Arc::new(RwLock::new(Vec::new()));
    for _ in 0..num_threads {
      let ts_arc = ts.clone();
      let expected_arc = expected.clone();
      let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        for _ in 0..num_data_points_per_thread {
          let time = rng.gen_range(0..10000);
          let dp = DataPoint::new(time, 1.0);
          ts_arc.append(time, 1.0);
          expected_arc.write().unwrap().push(dp);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    let compressed_blocks = ts.compressed_blocks.read().unwrap();
    let last_block = ts.last_block.read().unwrap();
    let initial_times = ts.initial_times.read().unwrap();

    assert_eq!(compressed_blocks.len(), num_blocks - 1);
    assert_eq!(last_block.len(), BLOCK_SIZE_FOR_TIME_SERIES);
    assert_eq!(initial_times.len(), num_blocks);

    let received = ts.get_time_series(0, u64::MAX);
    (*expected.write().unwrap()).sort();
    assert_eq!(*expected.read().unwrap(), received);
  }
}
