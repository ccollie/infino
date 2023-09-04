use std::io::Write;

use q_compress::data_types::NumberLike;
use q_compress::errors::QCompressError;
use q_compress::wrapped::{ChunkSpec, Compressor, Decompressor};
use q_compress::CompressorConfig;
use serde::{Deserialize, Serialize};

use lib::error::Error;
use lib::{marshal_var_i64, unmarshal_var_i64};

use crate::utils::{read_usize, write_usize};
use crate::{RuntimeError, RuntimeResult};
use crate::ts::time_series_block::TimeSeriesBlock;
use crate::utils::error::TsldbError;

pub struct DecompressorContext<'a> {
  data: &'a CompressedTimeseriesChunk,
  t_decompressor: Decompressor<i64>,
  v_decompressor: Decompressor<f64>,
}

impl<'a> DecompressorContext<'a> {
  fn new(data: &'a Vec<u8>) -> Self {
    let mut t_decompressor = Decompressor::<i64>::default();

    #[allow(unused_assignments)]
    let mut size: usize = 0;

    // read timestamp metadata
    let mut compressed = read_metadata(&mut t_decompressor, data)?;
    let mut v_decompressor = Decompressor::<f64>::default();

    compressed = read_metadata(&mut v_decompressor, compressed)?;

    Self {
      data,
      t_decompressor,
      v_decompressor,
    }
  }

  fn get_page_data(
    &mut self,
    buf: &[u8],
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
  ) -> RuntimeResult<&[u8]> {
    // we need to filter and append this data
    let (mut compressed, mut size) = read_usize(buf, "timestamp data size")?;

    let n = size;
    self.t_decompressor.write_all(&compressed[..size]).unwrap();
    let page_t = self.t_decompressor.data_page(n, size).map_err(map_err)?;

    compressed = &compressed[size..];

    (compressed, size) = read_usize(compressed, "value data size")?;
    self.v_decompressor.write_all(&compressed[..size]).unwrap();
    let page_v = self.v_decompressor.data_page(n, size).map_err(map_err)?;

    timestamps.extend(page_t);
    values.extend(page_v);
    Ok(compressed)
  }

  fn reset_buffers(&mut self) {
    self.timestamps.clear();
    self.values.clear();
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageMetadata {
  pub offset: usize,
  pub count: usize,
  pub t_min: i64,
  pub t_max: i64,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompressedTimeseriesChunk {
  pub data: Vec<u8>,
  pub pages: Vec<PageMetadata>,
  pub count: usize,
  pub max_points_per_page: usize,
  pub last_value: f64,
}

impl CompressedTimeseriesChunk {
  pub fn compress(timestamps: &[i64], values: &[f64]) -> RuntimeResult<Self> {
    // todo: calculate init buffer size
    // todo: ensure timestamps.len() == values.len()
    let mut data = Vec::with_capacity(1024);
    let pages = compress_series(&mut data, timestamps, values)?;
    let last_value = values[values.len() - 1];

    Self {
      data,
      pages,
      count: timestamps.len(),
      max_points_per_page: 0,
      last_value,
    }
  }

  pub fn len(&self) -> usize {
    self.count
  }

  pub fn is_empty(&self) -> bool {
    self.count == 0
  }

  pub fn overlaps_range(&self, start: i64, end: i64) -> bool {
    if let Some((first, last)) = self.get_timestamp_range() {
      return start <= last && end >= first;
    }
    false
  }

  pub fn first_timestamp(&self) -> Option<i64> {
    if self.is_empty() {
      return None;
    }
    Some(self.pages[0].t_min)
  }

  pub fn last_timestamp(&self) -> Option<i64> {
    if self.is_empty() {
      return None;
    }
    Some(self.pages[self.pages.len() - 1].t_max)
  }

  pub fn get_timestamp_range(&self) -> Option<(i64, i64)> {
    if self.is_empty() {
      return None;
    }
    let first = self.pages[0].t_min;
    let last = self.pages[self.pages.len() - 1].t_max;
    Some((first, last))
  }

  pub fn get_range(&self, start_ts: i64, end_ts: i64) -> Box<dyn Iterator<Item=DataPage>> {
    let iter = CompressedPageIterator::new(self, start_ts, end_ts, false);
    Box::new(iter)
  }

  pub fn get_rev_range(&self, start_ts: i64, end_ts: i64) -> Box<dyn Iterator<Item=DataPage>> {
      let iter =
        CompressedPageIterator::new(self, start_ts, end_ts, true);
        Box::new(iter)
  }

  pub fn remove_range(&mut self, start_ts: i64, end_ts: i64) -> RuntimeResult<usize> {
    // todo: better capacity calculation. Idead:: iterated over pages and calculate total size
    // / avg(t-max - tmin)
    let mut ts_buffer: Vec<i64> = Vec::with_capacity(1024);
    let mut value_buffer: Vec<f64> = Vec::with_capacity(1024);

    let max_ts = self.last_timestamp();
    let iter = CompressedPageIterator::new(self, 0, max_ts, false);
    for DataPage { timestamps, values } in iter  {
      let first_ts = timestamps[0];
      let last_ts = timestamps[timestamps.len() - 1];
      let non_overlapping = (first_ts >= start_ts && last_ts <= end_ts);
      // fast path
      let (stamps_, values_) = if  non_overlapping {
        (timestamps, values)
      } else {
        // slow path
        trim_series(timestamps, values, start_ts, end_ts)
      };
      ts_buffer.extend_from_slice(stamps_);
      value_buffer.extend_from_slice(values_);
    }
    //

    let deleted_count = self.count - ts_buffer.len();
    if deleted_count == 0 {
      return Ok(0);
    }

    // todo: calculate initial capacity
    let mut compressed_buf = Vec::with_capacity(1024);
    let page_info = compress_series(&mut compressed_buf, &ts_buffer, &value_buffer)?;
    let count = page_info.iter().map(|x| x.count).sum();
    self.pages = page_info;
    self.data = compressed_buf;
    self.count = count;
    self.last_value = value_buffer[value_buffer.len() - 1];

    Ok(deleted_count)
  }
}

impl Eq for CompressedTimeseriesChunk {}

impl TryFrom<&TimeSeriesBlock> for CompressedTimeseriesChunk {
  type Error = TsldbError;

  /// Compress the given time series block.
  fn try_from(time_series_block: &TimeSeriesBlock) -> Result<Self, Self::Error> {
    let time_series_data_points = &*time_series_block
        .get_time_series_data_points()
        .read()
        .unwrap();

    if time_series_data_points.is_empty() {
      error!("Cannot compress an empty time series block");
      return Err(TsldbError::EmptyTimeSeriesBlock());
    }
    let data_points_compressed_vec = compress_data_point_vector(time_series_data_points);

    Ok(Self::new_with_data_points_compressed_vec(
      data_points_compressed_vec,
    ))
  }
}


fn get_value_compressor(values: &[f64]) -> Compressor<f64> {
  Compressor::<f64>::from_config(q_compress::auto_compressor_config(
    values,
    q_compress::DEFAULT_COMPRESSION_LEVEL,
  ))
}

pub(crate) fn compress_series(
  buf: &mut Vec<u8>,
  timestamps: &[i64],
  values: &[f64],
) -> RuntimeResult<Vec<PageMetadata>> {
  let mut page_metas = Vec::with_capacity(timestamps.len() / DATA_PAGE_SIZE + 1);
  const DATA_PAGE_SIZE: usize = 3000;

  let mut count = timestamps.len();

  let mut data_page_sizes = Vec::with_capacity(count / DATA_PAGE_SIZE + 1);

  while count > 0 {
    let page_size = count.min(DATA_PAGE_SIZE);
    data_page_sizes.push(page_size);
    count -= page_size;
  }

  let chunk_spec = ChunkSpec::default().with_page_sizes(data_page_sizes.clone());

  // the caller ensures that timestamps are equally spaced, so we can use delta encoding
  let ts_compressor_config = CompressorConfig::default().with_delta_encoding_order(2);

  let mut ts_compressor = Compressor::<i64>::from_config(ts_compressor_config);

  let mut value_compressor = get_value_compressor(values);

  // timestamp metadata
  write_metadata(&mut ts_compressor, buf, timestamps, &chunk_spec)?;

  // write out value chunk metadata
  write_metadata(&mut value_compressor, buf, values, &chunk_spec)?;

  let mut idx = 0;
  for page_size in data_page_sizes.iter() {
    // Each page consists of
    // 1. count
    // 2. timestamp compressed body size
    // 3. timestamp page
    // 4. values compressed body size
    // 5. values page

    // 1.
    write_usize(buf, *page_size);

    // Update metadata
    let t_min = timestamps[idx];
    idx += page_size;
    let t_max = timestamps[idx - 1];

    page_metas.push(PageMetadata {
      offset: buf.len(),
      count: *page_size,
      t_min,
      t_max,
    });

    // 2.
    write_usize(buf, ts_compressor.byte_size());

    // 3.
    buf.extend(ts_compressor.drain_bytes());

    // 4.
    write_usize(buf, value_compressor.byte_size());

    // 5.
    buf.extend(value_compressor.drain_bytes());
  }

  Ok(page_metas)
}

pub struct DataPage<'a> {
    pub timestamps: &'a [i64],
    pub values: &'a [f64],
}

impl<'a> DataPage<'a> {
  pub fn len(&self) -> usize {
    self.timestamps.len()
  }

  pub fn is_empty(&self) -> bool {
      self.timestamps.is_empty()
  }

    pub fn first_timestamp(&self) -> Option<i64> {
        if self.timestamps.is_empty() {
            return None;
        }
        Some(self.timestamps[0])
    }

  pub fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
    if self.is_empty() {
      return false;
    }
    let first = self.timestamps[0];
    let last = self.timestamps[self.timestamps.len() - 1];
    start_ts <= last && end_ts >= first
  }

  pub fn trim(&self, start_ts: i64, end_ts: i64) -> (&[i64], &[f64]) {
    let stamps = &self.timestamps[0..];
    let start_idx = stamps
        .iter()
        .position(|&ts| ts >= start_ts)
        .unwrap_or(stamps.len());

    let end_idx = stamps
        .iter()
        .rev()
        .position(|&ts| ts <= end_ts)
        .unwrap_or(0);

    if start_idx > end_idx {
      return (&[], &[]);
    }

    let timestamps = &stamps[start_idx..end_idx];
    let values = &self.values[start_idx..end_idx];
    (timestamps, values)
  }

}

pub struct CompressedPageIterator<'a> {
  context: DecompressorContext<'a>,
  pages: &'a [PageMetadata],
  timestamps: Vec<i64>,
  values: Vec<f64>,
  index: usize,
  start: i64,
  end: i64,
  rev: bool,
  done: bool,
}

const DEFAULT_ITERATOR_BUF_SIZE: usize = 512;
static EMPTY_PAGEMETA_ARRAY: [PageMetadata; 0] = [];

impl<'a> CompressedPageIterator<'a> {
  pub fn new(
    compressed: &'a CompressedTimeseriesChunk,
    start: i64,
    end: i64,
    rev: bool
  ) -> CompressedPageIterator {
    let iter = CompressedPageIterator::new(compressed);
    let mut pages = &compressed.pages[0..];
    let page_idx = pages
      .iter()
      .position(|page| page.t_min <= start)
      .unwrap_or(pages.len() - 1);
    let end_page_idx = pages
      .iter()
      .rev()
      .position(|page| page.t_max <= end)
      .unwrap_or(0);

    let mut done = false;

    let pages = if page_idx > end_page_idx {
      done = true;
      pages = &EMPTY_PAGEMETA_ARRAY
    } else {
      &pages[page_idx..end_page_idx]
    };

    let page_start_idx = if rev {
      pages.len() - 1
    } else {
      0
    };

    CompressedPageIterator {
      context: DecompressorContext::new(&compressed.data),
      pages,
      index: page_start_idx,
      start,
      end,
      timestamps: Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE),
      values: Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE),
      rev,
      done
    }
  }

  fn next_page(&mut self) -> Option<&PageMetadata> {
    if self.done {
      return None;
    }
    let page = &self.pages[self.index];
    if self.rev {
      if self.index == 0 {
        self.done = true;
      } else {
        self.index -= 1;
      }
    } else {
      if self.index >= self.pages.len() {
        self.done = true;
      } else {
        self.index += 1;
      }
    }
    return Some(page);
  }

}

impl<'a> Iterator for CompressedPageIterator<'a> {
  type Item = DataPage<'a>;

  fn next(&mut self) -> Option<Self::Item> {
    let page = self.next_page();
    if page.is_none() {
      return None;
    }
    let page = page.unwrap();
    self.context.get_page_data(page.offset)?;
    let stamps = &self.timestamps[0..];

    // filter out timestamps out of range
    let start_idx = stamps
        .iter()
        .position(|&ts| ts >= self.start)
        .unwrap_or(stamps.len());

    let end_idx = stamps
        .iter()
        .rev()
        .position(|&ts| ts <= self.end)
        .unwrap_or(0);

    let timestamps = &stamps[start_idx..end_idx];
    let values = &self.values[start_idx..end_idx];

    let item = if self.rev {
      DataPage {
        timestamps: timestamps.reverse(),
        values: values.reverse(),
      }
    } else {
      DataPage {
        timestamps,
        values,
      }
    };

    Some(item)
  }
}

pub(super) fn estimate_size(tss: &[Timeseries]) -> usize {
  if tss.is_empty() {
    return 0;
  }
  // estimate size of labels
  let labels_size = tss
    .iter()
    .fold(0, |acc, ts| acc + ts.metric_name.serialized_size());
  let value_size = tss.iter().fold(0, |acc, ts| acc + ts.values.len() * 8);
  let timestamp_size = 8 * tss[0].timestamps.len();

  // Calculate the required size for marshaled tss.
  labels_size + value_size + timestamp_size
}

fn read_usize(slice: &[u8], context: &str) -> (&[u8], usize) {
  let byte_size = u32::from_be_bytes(slice[0..4].try_into().unwrap());
  (&slice[4..], byte_size as usize)
}

fn write_metadata<T: NumberLike>(
  compressor: &mut Compressor<T>,
  dest: &mut Vec<u8>,
  values: &[T],
  chunk_spec: &ChunkSpec,
) -> RuntimeResult<()> {
  // timestamp metadata
  compressor.header().map_err(map_err)?;
  compressor
    .chunk_metadata(values, chunk_spec)
    .map_err(map_err)?;
  write_usize(dest, compressor.byte_size());
  dest.extend(compressor.drain_bytes());
  Ok(())
}

fn read_metadata<'a, T: NumberLike>(
  decompressor: &mut Decompressor<T>,
  compressed: &'a [u8],
) -> RuntimeResult<&'a [u8]> {
  let (mut tail, size) = read_usize(compressed, "metatdata length")?;
  decompressor.write_all(&tail[..size]).unwrap();
  decompressor.header().map_err(map_err)?;
  decompressor.chunk_metadata().map_err(map_err)?;
  tail = &tail[size..];
  Ok(tail)
}

fn map_err(e: QCompressError) -> RuntimeError {
  RuntimeError::SerializationError(e.to_string())
}

fn map_unmarshal_err(e: Error, what: &str) -> RuntimeError {
  let msg = format!("error reading {what}: {:?}", e);
  RuntimeError::SerializationError(msg)
}

fn read_timestamp(compressed: &[u8]) -> RuntimeResult<(&[u8], i64)> {
  let (val, tail) = unmarshal_var_i64(compressed).map_err(|e| map_unmarshal_err(e, "timestamp"))?;
  Ok((tail, val))
}

fn write_timestamp(dest: &mut Vec<u8>, ts: i64) {
  marshal_var_i64(dest, ts);
}

#[cfg(test)]
mod tests {
  use super::*;

  fn test_series(tss: &[Timeseries]) {
    let mut buffer: Vec<u8> = Vec::new();
    compress_series(tss, &mut buffer).unwrap();
    let tss2 = deserialize_series_between(&buffer, 0, 100000).unwrap();

    assert_eq!(
      tss, tss2,
      "unexpected timeseries unmarshaled\ngot\n{:?}\nwant\n{:?}",
      tss2[0], tss[0]
    )
  }

  // Single series
  #[test]
  fn single_series() {
    let series = Timeseries::default();
    test_series(&[series]);

    let mut series = Timeseries::default();
    series.metric_name.metric_group = "foobar".to_string();
    series.metric_name.add_tag("tag1", "value1");
    series.metric_name.add_tag("tag2", "value2");

    series.values = vec![1.0, 2.0, 3.234];
    series.timestamps = Arc::new(vec![10, 20, 30]);
    test_series(&[series]);
  }

  #[test]
  fn multiple_series() {
    let mut series = Timeseries::default();
    series.metric_name.metric_group = "foobar".to_string();
    series.metric_name.add_tag("tag1", "value1");
    series.metric_name.add_tag("tag2", "value2");

    series.values = vec![1.0, 2.34, -33.0];
    series.timestamps = Arc::new(vec![0, 10, 20]);

    let mut series2 = Timeseries::default();
    series2.metric_name.metric_group = "baz".to_string();
    series2.metric_name.add_tag("tag12", "value13");

    series2.values = vec![4.0, 1.0, -2.34];
    series2.timestamps = Arc::new(vec![0, 10, 20]);

    test_series(&[series, series2]);
  }
}
