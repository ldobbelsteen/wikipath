#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_panics_doc)]

mod database;
mod dump;
mod parse;
mod search;

pub use database::Database;
pub use database::Metadata;
pub use database::PageId;
pub use database::WriteTransaction;
pub use dump::TableDumpFiles;
pub use parse::cleanup_redirects;
