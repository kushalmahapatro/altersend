//! Pure business logic for AlterSend — state machine, join codes, and UI derivations.
//! No I/O, networking, or platform APIs.

mod deep_link;
mod download;
mod draft;
mod format;
mod join_code;
mod page_ui;
mod reducer;
mod share;
mod types;

pub use deep_link::*;
pub use download::*;
pub use draft::*;
pub use format::*;
pub use join_code::*;
pub use page_ui::*;
pub use reducer::*;
pub use share::*;
pub use types::*;
