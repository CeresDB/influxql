#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![allow(clippy::clone_on_ref_ptr)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

pub mod optimize;
pub mod dictionary;
pub mod display;
pub mod string;

/// This has a collection of testing helper functions
pub mod test_util;
