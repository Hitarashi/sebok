# sebok (সেবক)

[![Crates.io](https://img.shields.io/crates/v/sebok.svg)](https://crates.io/crates/sebok)
[![Documentation](https://docs.rs/sebok/badge.svg)](https://docs.rs/sebok)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)

Universal, runtime-agnostic asynchronous HTTP/1.1 server engine and connection lifecycle manager for Rust.

Named after the Bengali word **সেবক** (*sebok* - server, attendant, or provider), `sebok` gives you complete stack ownership over HTTP/1.1 connection loops, framing, chunked encoding, and streaming bodies without being tied to any specific asynchronous runtime.

---

## Features

- **100% Runtime Agnostic**: Zero runtime dependencies. Runs on Tokio, smol, async-std, glommio, or in-memory duplex streams.
- **RFC 9112 HTTP/1.1 Compliance**:
  - Connection Keep-Alive and persistent connection reuse.
  - Zero-copy header and request line parsing via `httparse`.
  - Automatic `Expect: 100-continue` handshake.
  - Full chunked transfer encoding (`Transfer-Encoding: chunked`) decoding and encoding.
  - Unread body draining to ensure persistent socket alignment.
- **Defensive Protocol Security**:
  - Protection against Slowloris and header overflow attacks.
  - Configurable maximum header size (default 64 KB) and maximum header count (default 100).
- **First-Class Streaming**:
  - Streaming request body implementing `AsyncRead`.
  - Outgoing response body supporting `Empty`, `Bytes`, and indefinite chunked streams (`Stream`).
- **Standard Types**: Built on the official, zero-dependency `http` (v1) crate.

---

## Installation

Add `sebok` to your `Cargo.toml`:

```toml
[dependencies]
sebok = "0.1.0"
```

---

## Quick Start

```rust
use http::{Request, Response, StatusCode};
use sebok::{HttpService, RequestBody, ResponseBody, ServerConfig, serve_connection};

async fn handle_request(req: Request<RequestBody>) -> Result<Response<ResponseBody>, sebok::SebokError> {
    let body = format!("Hello! Requested path: {}", req.uri().path());
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(ResponseBody::from(body))
        .unwrap())
}

// Serve any stream implementing futures_io::AsyncRead + AsyncWrite
// Works with Tokio, smol, async-std, or mock test streams!
async fn handle_client<S>(stream: S) -> Result<(), sebok::SebokError>
where
    S: futures_io::AsyncRead + futures_io::AsyncWrite + Unpin + Send + 'static,
{
    let config = ServerConfig::default();
    serve_connection(stream, handle_request, config).await
}
```

---

## Standards Compliance

- **RFC 9110**: HTTP Semantics.
- **RFC 9112**: HTTP/1.1 Messaging, Framing, Chunked Transfer Coding, and Connection Management.
- **RFC 6585**: Additional HTTP Status Codes (431 Request Header Fields Too Large).

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
