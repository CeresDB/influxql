// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
#![allow(clippy::derive_partial_eq_without_eq, clippy::needless_borrow)]

/// This module imports the generated protobuf code into a Rust module
/// hierarchy that matches the namespace hierarchy of the protobuf
/// definitions
pub mod influxdata {
    pub mod iox {
        pub mod querier {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.querier.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.querier.v1.serde.rs"
                ));
            }
        }
    }
}
