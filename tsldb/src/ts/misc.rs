pub(crate) fn trim_series(series: &mut Vec<f64>, max_size: usize) {
    if series.len() > max_size {
        series.drain(0..(series.len() - max_size));
    }
}
