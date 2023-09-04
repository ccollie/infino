pub enum DuplicateStatus {
    Ok,
    Err,
}
// This function will decide according to the policy how to handle duplicate sample, the `newSample`
// will contain the data that will be kept in the database.
pub fn handle_duplicate_sample(policy: DuplicatePolicy, old_sample: Sample, new_sample: &mut Sample) {
    let has_nan = oldSample.value.is_nan() || newSample.value.is_nan();
    if has_nan && policy != DuplicatePolicy::Block {
        // take the valid sample regardless of policy
        if new_sample.value.is_nan() {
            new_sample.value = old_sample.value;
        }
        return DuplicateStatus::Ok;
    }
    match policy {
        DuplicatePolicy::Block => DuplicateStatus::Err,
        DuplicatePolicy::First => {
            // keep the first sample
            new_sample.value = old_sample.value;
            DuplicateStatus::Ok
        }
        DuplicatePolicy::Last => {
            // keep the last sample
            DuplicateStatus::Ok
        }
        DuplicatePolicy::Min => {
            // keep the min sample
            if old_sample.value < new_sample.value {
                new_sample.value = old_sample.value;
            }
            DuplicateStatus::Ok
        }
        DuplicatePolicy::Max => {
            // keep the max sample
            if old_sample.value > new_sample.value {
                new_sample.value = old_sample.value;
            }
            DuplicateStatus::Ok
        }
        DuplicatePolicy::Sum => {
            // sum the samples
            new_sample.value += old_sample.value;
            DuplicateStatus::Ok
        }
    }
    DuplicateStatus::Err
}

pub fn trim_data(timestamps: &[i64], values: &[f64], start_ts: i64, end_ts: i64) -> (&[i64], &[f64]) {
    let stamps = timestamps[0..];
    let start_idx = stamps
        .iter()
        .position(|&ts| ts >= start)
        .unwrap_or(timestamps.len());

    let end_idx = stamps
        .iter()
        .rev()
        .position(|&ts| ts <= end)
        .unwrap_or(0);

    if start_idx > end_idx {
        return (&[], &[]);
    }

    let timestamps = &stamps[start_idx..end_idx];
    let values = &values[start_idx..end_idx];
    (timestamps, values)
}