
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UncompressedChunk {
    pub(super) timestamps: Vec<i64>,
    pub(super) values: Vec<f64>,
    base_timestamp: i64,
    size: usize,
}

impl UncompressedChunk {
    pub fn new(timestamps: Vec<i64>, values: Vec<f64>) -> Self {
        Self {
            timestamps,
            values,
            base_timestamp: 0,
            size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.timestamps.len() == self.size
    }

    pub fn first_timestamp(&self) -> Option<i64> {
        if self.timestamps.is_empty() {
            return None;
        }
        Some(self.timestamps[0])
    }

    pub fn remove_range(&mut self, start_ts: i64, end_ts: i64) -> usize{
        let start_idx = self.timestamps.iter()
            .position(|&ts| ts >= start_ts)
            .unwrap_or(self.timestamps.len());
        let end_idx = self.timestamps.iter().rev()
            .position(|&ts| ts <= end_ts)
            .unwrap_or(0);

        if start_idx >= end_idx {
            return 0;
        }

        let _ = self.values.drain(start..end);
        let iter = self.timestamps.drain(start..end);
        iter.count()
    }

    pub fn into_compressed(self) -> CompressedChunk {
        let mut compressed_chunk = CompressedChunk::default();
        compressed_chunk.compress(self.timestamps, self.values);
        compressed_chunk
    }

    pub fn split(self, index: usize) -> (Self, Self) {
        let (left_timestamps, right_timestamps) = self.timestamps.split_at(index);
        let (left_values, right_values) = self.values.split_at(index);
        (
            Self::new(left_timestamps.to_vec(), left_values.to_vec()),
            Self::new(right_timestamps.to_vec(), right_values.to_vec()),
        )
    }

    pub fn add_sample(&mut self, timestamp: i64, value: f64) -> bool {
        if self.is_full() {
            return false;
        }
        if self.is_empty() {
            self.base_timestamp = timestamp;
        }
        self.timestamps.push(timestamp);
        self.values.push(value);
        true
    }

    pub fn upsert_sample(&mut self, sample: &Sample, duplicate_policy: DuplicatePolicy) -> RuntimeResult<usize> {
        let ts = sample.timestamp;

        let mut i = 0;
        let mut sample_ts = 0;
        // eliminate bounds checks
        let timestamps = &self.timestamps[0..];

        while i < timestamps.len() {
            sample_ts = timestamps[i];
            if ts <= sample_ts {
                break;
            }
            i += 1;
        }

        // update value in case timestamp exists
        if sample_ts != 0 && ts == sample_ts {
            let current = Sample {
                timestamp: sample_ts,
                value: self.values[i],
            };
            let cr = handleDuplicateSample(duplicatePolicy, &current, &sample);
            if cr != CR_OK {
                return CR_ERR;
            }
            self.values[i] = sample.value;
            return CR_OK;
        }

        if i == 0 {
            self.base_timestamp = ts;
        }

        if i < timestamps.len() {
            self.timestamps.insert(i, ts);
            self.values.insert(i, sample.value);
        } else {
            self.timestamps.push(ts);
            self.values.push(sample.value);
        }
        self.size += f64::SIZE + i64::SIZE;

        *size = 1;
        return CR_OK;
    }


    pub fn add_sample_(&mut self, idx: usize, timestamp: i64, value: f64) {

        // todo: !!!
        self.timestamps.push(timestamp);
        self.values.push(value);
    }
}