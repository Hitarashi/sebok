//! Universal, runtime-agnostic asynchronous HTTP/1.1 server engine and connection lifecycle manager.
//!
//! Provides zero-copy HTTP/1.1 framing, connection persistence (Keep-Alive), chunked transfer encoding,
//! and defensive protocol security over generic asynchronous streams.

pub mod body;
pub mod buffered;
pub mod chunked;
pub mod config;
pub mod connection;
pub mod date;
pub mod error;
pub mod service;

pub use body::{RequestBody, ResponseBody};
pub use config::{
    DEFAULT_KEEP_ALIVE_TIMEOUT, DEFAULT_MAX_HEADER_SIZE, DEFAULT_MAX_HEADERS_COUNT, ServerConfig,
    ServerConfigBuilder,
};
pub use connection::serve_connection;
pub use date::format_http_date;
pub use error::SebokError;
pub use service::{BoxFuture, HttpService};

/// Convenient prelude re-exporting core server types and traits.
pub mod prelude {
    pub use crate::{
        BoxFuture, HttpService, RequestBody, ResponseBody, SebokError, ServerConfig,
        ServerConfigBuilder, serve_connection,
    };
}
