use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
/// Enum for various errors in Tsldb.
pub enum TsldbError {
  #[error("Invalid size. Expected {0}, Received {1}.")]
  InvalidSize(usize, usize),

  #[error("Already at full capacity. Max capacity {0}.")]
  CapacityFull(usize),

  #[error("Cannot read directory {0}.")]
  CannotReadDirectory(String),

  #[error("Cannot find index metadata in directory {0}.")]
  CannotFindIndexMetadataInDirectory(String),

  #[error("The directory {0} is not an index directory.")]
  NotAnIndexDirectory(String),

  #[error("Time series block is empty - cannot be compressed.")]
  EmptyTimeSeriesBlock(),

  #[error("Cannot decode time series. {0}")]
  CannotDecodeTimeSeries(String),

  #[error("Invalid configuration. {0}")]
  InvalidConfiguration(String),

  #[error("Encoding error. {0}")]
  EncodingError(String),

  #[error("Serialization error. {0}")]
  CannotSerialize(String),

  #[error("Cannot deserialize. {0}")]
  CannotDeserialize(String),

  #[error("Cannot decompress. {0}")]
  DecompressionFailed(String),
}
