use std::io::Write;
use std::mem::size_of;
use std::sync::Arc;

use q_compress::data_types::NumberLike;
use q_compress::errors::QCompressError;
use q_compress::wrapped::{ChunkSpec, Compressor, Decompressor};
use q_compress::CompressorConfig;

use lib::error::Error;
use lib::{marshal_var_i64, unmarshal_var_i64};

use crate::utils::{read_usize, write_usize};
use crate::{MetricName, RuntimeError, RuntimeResult, Timeseries};

type DecompressedDataPredicate = fn(&[i64], &[f64]) -> RuntimeResult<bool>;

pub struct DecompressorContext<'a> {
  data: &'a CompressedTimeseries,
  t_decompressor: Decompressor<i64>,
  v_decompressor: Decompressor<f64>,
}

impl<'a> DecompressorContext<'a> {
  fn new(data: &'a vec<u8>) -> Self {
    let mut t_decompressor = Decompressor::<i64>::default();

    #[allow(unused_assignments)]
    let mut size: usize = 0;

    // read timestamp metadata
    let mut compressed = read_metadata(&mut t_decompressor, compressed)?;
    let v_decompressor = Decompressor::<f64>::default();

    compressed = read_metadata(v_decompressor, compressed)?;

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

  fn iterate_while(&mut self, start_ofs: usize, pred: DecompressedDataPredicate) -> RuntimeResult<usize> {
    let mut count = 0;
    let mut buf = self.data[start_ofs..];
    let mut timestamps = Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE);
    let mut values = Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE);

    loop {
      buf = self.get_page_data(buf, &mut timestamps, &mut values)?;
      if self.timestamps.is_empty() {
        return Ok(count);
      }
      if pred(&self.timestamps, &self.values)? {
        count += self.timestamps.len();
      } else {
        break;
      }
      if buf.is_empty() {
        break;
      }
      timestamps.clear();
      values.clear();
    }
    Ok(count)
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompressedTimeseries {
  pub data: Vec<u8>,
  pub pages: Vec<PageMetadata>,
  pub count: usize,
  pub max_points_per_page: usize,
  pub last_value: f64,
}

impl CompressedTimeseries {
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

  pub fn get_range(
    &self,
    start: i64,
    end: i64,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
  ) -> RuntimeResult<usize> {
    if !self.overlaps_range(start, end) {
        return Ok(0);
    }
    let mut decompressor = DecompressorContext::new(&self.data);

    // find first page in range
    let mut page_idx: usize = 0;

    for page in self.pages {
      if page.t_min <= start {
        break;
      }
      page_idx += 1;
    }

    let mut pages = &self.pages[page_idx..];
    for page in pages {
      if page.t_max >= end {
        break;
      }
      let mut buf = &self.data[page.offset..];
      context.get_page_data(buf, timestamps, values)?;

      // todo: this can probably be faster
      let filtered = context.timestamps
        .iter()
        .zip(context.values)
        .filter(|(t, _)| (start..end).contains(t))
        .collect::<Vec<_>>();

      timestamps.extend(filtered.iter().map(|(t, _)| *t));
      values.extend(filtered.into_iter().map(|(_, v)| v));

      context.reset_buffers();
      if timestamps[timestamps.len() - 1] >= end {
        break;
      }
    }

    Ok(timestamps.len() - start_count)
  }

  pub fn remove_range(&mut self, start_ts: i64, end_ts: i64) -> usize {
    if !self.overlaps_range(start_ts, end_ts) {
      return 0;
    }
    let mut timestamps = Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE);
    let mut values = Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE);

    let mut page_idx: usize = 0;
    for page in self.pages {
      if page.t_min <= start_ts {
        break;
      }
      page_idx += 1;
    }
    let mut pages = &self.pages[page_idx..];
    let mut decompressor = DecompressorContext::new(&self.data);

    let mut count = 0;
    for page in pages {
      if page.t_max >= end_ts {
        break;
      }

      let mut buf = &self.data[page.offset..];
      context.get_page_data(buf, timestamps, values)?;

      // todo: this can probably be faster
      let filtered = context.timestamps
          .iter()
          .zip(context.values)
          .filter(|(t, _)| (start..end).contains(t))
          .collect::<Vec<_>>();

      count += page.count;
    }
    self.pages.drain(page_idx..page_idx + pages.len());
    self.count -= count;
    count
  }
}

pub struct BufferedWriter {
    buf: Vec<u8>,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    v_compressor: Compressor<f64>,
    t_compressor: Compressor<i64>,
    count: usize,
    max_points_per_page: usize,
    last_value: f64,
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
  let page_metas = Vec::with_capacity(timestamps.len() / DATA_PAGE_SIZE + 1);
  const DATA_PAGE_SIZE: usize = 3000;

  let series_count = series.len();
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
  write_metadata(value_compressor, buf, alues, &chunk_spec)?;

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
      count: page_size,
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


pub struct CompressedChunkIterator<'a> {
  parent: &'a CompressedTimeseries,
  context: DecompressorContext<'a>,
  page_index: usize,
  iter_ts: i64,
  index: usize,
  start: i64,
  end: i64,
}

const DEFAULT_ITERATOR_BUF_SIZE: usize = 32;

impl CompressedChunkIterator<'_> {
  pub fn new(compressed: &CompressedTimeseries) -> CompressedChunkIterator {
    CompressedChunkIterator {
      parent: compressed,
      context: DecompressorContext::new(&compressed.data),
      page_index: 0,
      index: 0,
      start: 0,
      iter_ts: 0,
      end: i64::MAX,
    }
  }

  pub fn with_range(
    compressed: &CompressedTimeseries,
    start: i64,
    end: i64,
  ) -> CompressedChunkIterator {
    let iter = CompressedChunkIterator::new(compressed);
    iter.set_range(start, end);
  }
}

impl Iterator for CompressedChunkIterator<'_> {
  type Item = DataPoint;

  fn next(&mut self) -> Option<Self::Item> {
    if self.page_index >= self.parent.pages.len() {
      return None;
    }
    if index >= self.context.timestamps.len() {
      while let Some(page) = self.next_page() {
        self.context.get_page_data(page)?;
        if self.context.timestamps.is_empty() {
          return continue;
        }
        self.index = 0;
        if self.context.timestamps.is_empty() {
          return None;
        }
      }
      self.index = 0;
      if count == 0 {
        return None;
      }
    }
    let ts = self.context.timestamps[self.index];
    let v = self.context.values[self.index];
    self.index += 1;
    Some(DataPoint { ts, v })
  }

  fn next_page(&self) -> Option<&PageMetadata> {
    while self.page_index <= self.parent.pages.len() {
        let page = &self.parent.pages[self.page_index];
        if page.t_max < self.iter_ts {
            self.page_index += 1;
            continue;
        }
        if page.t_min > self.end {
            return None;
        }
        self.iter_ts = page.t_min;
        return Some(page);
    }
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
