# `sebok` Agent Instructions

## Scope

`sebok` is a universal, runtime-agnostic asynchronous HTTP/1.1 server engine and connection lifecycle manager for Rust.

## Design Rules

1. **Total Runtime Independence**:
    - The crate must compile with zero async runtime dependencies (no Tokio, no smol, no async-std in dependencies).
    - All network I/O must operate exclusively over the standard `futures_io::{AsyncRead, AsyncWrite}` traits.
    - Any transport stream satisfying `AsyncRead + AsyncWrite + Unpin + Send + 'static` can be served.

2. **HTTP Standards & Protocol Compliance**:
    - Conform strictly to RFC 9110 (HTTP Semantics) and RFC 9112 (HTTP/1.1).
    - Support persistent connections (Connection Keep-Alive) by default in HTTP/1.1 and on explicit request in HTTP/1.0.
    - Support `Expect: 100-continue` handshake before reading large request bodies.
    - Support chunked transfer encoding (`Transfer-Encoding: chunked`) for streaming requests and responses.
    - Properly drain unread request body bytes before accepting subsequent pipelined or keep-alive requests on the same connection.
    - Support `Connection: close` termination.

3. **Defensive Security & Resource Bounds**:
    - Guard against header bombs and Slowloris attacks with bounded maximum header size (default 64 KB) and maximum header count (default 100).
    - Enforce timeouts on initial header arrival and keep-alive idle intervals.
    - Never panic on malformed or malicious wire data; return proper 4xx HTTP responses (`400 Bad Request`, `431 Request Header Fields Too Large`, `413 Payload Too Large`, `505 HTTP Version Not Supported`).

4. **Framework Agnosticism**:
    - Do not mention any other web frameworks in rustdocs, codebase comments, identifiers, or error messages.
    - Reference only RFCs, W3C standards, official specifications, and Rust types.

5. **Code Style & Quality**:
    - Follow Google's Rust Style Guide.
    - Do not suppress Clippy warnings inline (`#[allow(...)]` or `#[expect(...)]`).
    - Avoid using leading underscores (`_`) on identifiers, variables, parameters, or struct fields.
    - Code formatting must be verified via `cargo +nightly fmt --check`.

## Testing Requirements

- Test HTTP/1.1 methods (GET, POST, PUT, DELETE, HEAD, OPTIONS).
- Test persistent Keep-Alive connections handling multiple consecutive requests.
- Test `Connection: close` termination.
- Test `Expect: 100-continue` flow.
- Test fixed-length body decoding and chunked transfer decoding/encoding.
- Test header limit enforcement (oversized headers, too many headers).
- Test in-memory duplex streams without opening real network sockets.
