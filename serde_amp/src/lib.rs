#![cfg_attr(test, expect(unused_crate_dependencies))]
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod de;
mod error;
mod ser;

/// The box type that [`to_box`] produces and [`from_box`] reads, re-exported
/// from `amp-protocol` so that callers use the version this crate was built
/// against.
pub use amp_protocol::AmpBox;
pub use de::from_box;
pub use error::Error;
pub use ser::to_box;
