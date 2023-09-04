mod compress;
mod encoding;
mod float;
mod int;
mod nearest_delta;
mod nearest_delta2;

pub use compress::*;
pub use encoding::*;
pub use float::*;
pub use int::*;
pub use nearest_delta::*;
pub use nearest_delta2::*;

// todo: move to sep file ?
#[cfg(test)]
mod int_test;
#[cfg(test)]
mod nearest_delta_test;
