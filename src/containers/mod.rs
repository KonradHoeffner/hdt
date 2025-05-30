/// In-memory RDF representation.
pub mod rdf;

/// Variable length numbers.
pub mod vbyte;

// byte containers
mod adj_list;
mod bitmap;
mod sequence;

// control info section reader
mod control_info;

pub use adj_list::AdjList;
pub use bitmap::{Bitmap, BitmapReadError};
pub use control_info::{ControlInfo, ControlInfoReadError, ControlType};
pub use sequence::{Sequence, SequenceReadError};
