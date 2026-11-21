# `sebok` Context & Architecture

## Vision & Scope

`sebok` ("সেবক" - server, attendant, or provider) is a universal, runtime-agnostic asynchronous HTTP/1.1 server engine and connection lifecycle manager for Rust.

It operates independently of any network runtime or framework executor, providing zero-copy HTTP/1.1 framing, connection persistence, streaming body management, and defensive protocol security over generic asynchronous byte streams.

## Specification & Standards Compliance

- **RFC 9110**: HTTP Semantics (Methods, Status Codes, Header Fields, Content Negotiation).
- **RFC 9112**: HTTP/1.1 (Message Format, Chunked Transfer Coding, Connection Management, Keep-Alive, Pipelining, 100-Continue).
- **RFC 6585**: Additional HTTP Status Codes (`431 Request Header Fields Too Large`).

## Core Architecture

1. **Runtime-Neutral Transport Layer**:
   - Operates on any stream implementing `futures_io::AsyncRead + futures_io::AsyncWrite + Unpin + Send + 'static`.
   - Compatible with Tokio (via compat layer), smol, async-std, glommio, or in-memory duplex streams.

2. **Connection Lifecycle (`serve_connection`)**:
   - Reads and buffers request header blocks up to configurable bounds (`max_header_size`, default 64 KB).
   - Zero-copy parses request lines and headers using `httparse`.
   - Handles `Expect: 100-continue` handshake prior to reading message bodies.
   - Decodes message bodies according to `Content-Length` or `Transfer-Encoding: chunked`.
   - Passes standard `http::Request<RequestBody>` to the application's `HttpService`.
   - Encodes and flushes `http::Response<ResponseBody>` to the socket.
   - Enforces connection reuse according to HTTP/1.1 rules, draining unread request body bytes before next message processing.

3. **Streaming Bodies**:
   - `RequestBody`: Implements `AsyncRead`, seamlessly handling fixed-length slices and decoding chunked wire streams.
   - `ResponseBody`: Supports empty bodies, fixed-size byte buffers, and arbitrary chunked asynchronous streams.

4. **Service Interface (`HttpService`)**:
   - Generic async contract mapping `http::Request<RequestBody>` to `http::Response<ResponseBody>`.
   - Blanket implementation for any compatible async closure.

## Public API Surface

- `fn serve_connection<S, H>(stream: S, service: H, config: ServerConfig) -> Result<(), SebokError>`
- `trait HttpService`: Async request handling trait.
- `struct ServerConfig` & `struct ServerConfigBuilder`: Connection limits, timeouts, and buffer settings.
- `struct RequestBody`: Streaming request payload reader.
- `enum ResponseBody`: Encapsulated outgoing payload (Empty, Bytes, Stream).
- `enum SebokError`: Strongly typed protocol and I/O error representation.
