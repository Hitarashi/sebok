//! Connection lifecycle, message framing, and persistent Keep-Alive processing.

use std::{
    str::FromStr,
    sync::{Arc, atomic::Ordering},
    time::SystemTime,
};

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{AsyncReadExt, AsyncWriteExt};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, Uri, Version};

use crate::{
    body::{RequestBody, ResponseBody, SharedChunkedDecoder, TrackedTake},
    buffered::SharedReader,
    chunked::{encode_chunk, terminal_chunk},
    config::ServerConfig,
    date::format_http_date,
    error::SebokError,
    service::HttpService,
};

enum BodyDrainer<R> {
    Empty,
    Fixed(Arc<std::sync::atomic::AtomicU64>),
    Chunked(SharedChunkedDecoder<SharedReader<R>>),
}

/// Serves an asynchronous stream connection, executing the HTTP/1.1 request-response lifecycle.
///
/// Supports persistent connections (Keep-Alive), chunked request/response bodies, and `100 Continue`.
pub async fn serve_connection<S, H>(
    stream: S,
    service: H,
    config: ServerConfig,
) -> Result<(), SebokError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: HttpService,
{
    let service = Arc::new(service);
    let (read_half, mut write_half) = stream.split();
    let shared_reader = SharedReader::new(read_half);

    loop {
        // 1. Read request headers from stream into buffer until CRLF CRLF is found
        let header_end = {
            let mut guard = shared_reader.lock().await;
            match guard.read_headers(config.max_header_size).await {
                Ok(Some(end)) => end,
                Ok(None) => {
                    // Connection closed cleanly by client at message boundary
                    return Ok(());
                }
                Err(err) => {
                    if matches!(err, SebokError::HeaderTooLarge) {
                        let _ = write_half
                            .write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        let _ = write_half.flush().await;
                    }
                    return Err(err);
                }
            }
        };

        // 2. Parse request line and headers with httparse
        let mut header_storage = vec![httparse::EMPTY_HEADER; config.max_headers_count];
        let mut parsed_request = httparse::Request::new(&mut header_storage);

        let (
            method,
            uri,
            version,
            headers,
            content_length,
            is_chunked,
            expect_continue,
            client_conn_close,
            client_conn_keep_alive,
        ) = {
            let mut guard = shared_reader.lock().await;
            let header_bytes = guard.headers_slice(header_end);

            let parse_status = match parsed_request.parse(header_bytes) {
                Ok(status) => status,
                Err(parse_err) => {
                    drop(guard);
                    let _ = write_half
                        .write_all(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    let _ = write_half.flush().await;
                    return Err(SebokError::from(parse_err));
                }
            };

            if parse_status.is_partial() {
                drop(guard);
                let _ = write_half
                    .write_all(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                    .await;
                let _ = write_half.flush().await;
                return Err(SebokError::Protocol(
                    "Incomplete HTTP request header".to_string(),
                ));
            }

            let method_str = parsed_request.method.unwrap_or("GET");
            let method = Method::from_str(method_str)
                .map_err(|err| SebokError::Protocol(format!("Invalid HTTP method: {err}")))?;

            let uri_str = parsed_request.path.unwrap_or("/");
            let uri = Uri::from_str(uri_str)
                .map_err(|err| SebokError::Protocol(format!("Invalid request URI: {err}")))?;

            let version = match parsed_request.version {
                Some(1) => Version::HTTP_11,
                Some(0) => Version::HTTP_10,
                _ => Version::HTTP_11,
            };

            let mut headers = HeaderMap::with_capacity(parsed_request.headers.len());
            let mut content_length: Option<u64> = None;
            let mut is_chunked = false;
            let mut expect_continue = false;
            let mut client_connection_close = version == Version::HTTP_10;
            let mut client_connection_keep_alive = version == Version::HTTP_11;

            for header in parsed_request.headers.iter() {
                if let (Ok(name), Ok(val)) = (
                    HeaderName::from_bytes(header.name.as_bytes()),
                    HeaderValue::from_bytes(header.value),
                ) {
                    let name_lower = header.name.to_ascii_lowercase();
                    if name_lower == "content-length" {
                        if let Ok(len_str) = std::str::from_utf8(header.value) {
                            content_length = len_str.trim().parse().ok();
                        }
                    } else if name_lower == "transfer-encoding" {
                        if let Ok(te_str) = std::str::from_utf8(header.value) {
                            if te_str.to_ascii_lowercase().contains("chunked") {
                                is_chunked = true;
                            }
                        }
                    } else if name_lower == "expect" {
                        if let Ok(exp_str) = std::str::from_utf8(header.value) {
                            if exp_str.trim().eq_ignore_ascii_case("100-continue") {
                                expect_continue = true;
                            }
                        }
                    } else if name_lower == "connection" {
                        if let Ok(conn_str) = std::str::from_utf8(header.value) {
                            let conn_lower = conn_str.to_ascii_lowercase();
                            if conn_lower.contains("close") {
                                client_connection_close = true;
                                client_connection_keep_alive = false;
                            } else if conn_lower.contains("keep-alive") {
                                client_connection_keep_alive = true;
                                client_connection_close = false;
                            }
                        }
                    }
                    headers.append(name, val);
                }
            }

            guard.advance_cursor(header_end);

            (
                method,
                uri,
                version,
                headers,
                content_length,
                is_chunked,
                expect_continue,
                client_connection_close,
                client_connection_keep_alive,
            )
        };

        // Prevent request smuggling: both Content-Length and chunked Transfer-Encoding
        if content_length.is_some() && is_chunked {
            let _ = write_half
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                )
                .await;
            let _ = write_half.flush().await;
            return Err(SebokError::Protocol(
                "Request contains both Content-Length and Transfer-Encoding".to_string(),
            ));
        }

        // 3. Handle Expect: 100-continue handshake if requested
        if expect_continue && config.expect_continue {
            write_half
                .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                .await?;
            write_half.flush().await?;
        }

        // 4. Construct RequestBody and drain handle
        let (request_body, drainer) = if is_chunked {
            let shared_decoder = SharedChunkedDecoder::new(shared_reader.clone());
            let body = RequestBody::from_reader(shared_decoder.clone());
            (body, BodyDrainer::Chunked(shared_decoder))
        } else if let Some(length) = content_length {
            if length == 0 {
                (RequestBody::empty(), BodyDrainer::Empty)
            } else {
                let (tracked, remaining_tracker) = TrackedTake::new(shared_reader.clone(), length);
                let body = RequestBody::from_reader(tracked);
                (body, BodyDrainer::Fixed(remaining_tracker))
            }
        } else {
            (RequestBody::empty(), BodyDrainer::Empty)
        };

        // 5. Build http::Request and dispatch to service
        let mut http_req = Request::builder().method(method).uri(uri).version(version);

        for (name, val) in &headers {
            http_req = http_req.header(name, val);
        }

        let request = http_req
            .body(request_body)
            .map_err(|err| SebokError::Protocol(format!("Failed to build request: {err}")))?;

        let response = match service.call(request).await {
            Ok(resp) => resp,
            Err(service_err) => {
                let status = service_err.status_code();
                let status_line = format!(
                    "HTTP/1.1 {} {}\r\n",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Error")
                );
                let _ = write_half.write_all(status_line.as_bytes()).await;
                let _ = write_half
                    .write_all(b"Connection: close\r\nContent-Length: 0\r\n\r\n")
                    .await;
                let _ = write_half.flush().await;
                return Err(service_err);
            }
        };

        // 6. Evaluate connection persistence
        let will_keep_alive = config.keep_alive
            && !client_conn_close
            && client_conn_keep_alive
            && !response_requests_close(&response);

        // 7. Write HTTP/1.1 response status line and headers
        let status = response.status();
        let reason = status.canonical_reason().unwrap_or("OK");
        let mut response_head = format!("HTTP/1.1 {} {reason}\r\n", status.as_u16());

        for (name, value) in response.headers() {
            if let Ok(val_str) = value.to_str() {
                response_head.push_str(name.as_str());
                response_head.push_str(": ");
                response_head.push_str(val_str);
                response_head.push_str("\r\n");
            }
        }

        if !response.headers().contains_key(http::header::DATE) {
            response_head.push_str("date: ");
            response_head.push_str(&format_http_date(SystemTime::now()));
            response_head.push_str("\r\n");
        }

        if will_keep_alive {
            if !response.headers().contains_key(http::header::CONNECTION) {
                response_head.push_str("connection: keep-alive\r\n");
            }
        } else {
            response_head.push_str("connection: close\r\n");
        }

        let (_, body) = response.into_parts();
        match body {
            ResponseBody::Empty => {
                if !response_head.contains("content-length:") {
                    response_head.push_str("content-length: 0\r\n");
                }
                response_head.push_str("\r\n");
                write_half.write_all(response_head.as_bytes()).await?;
            }
            ResponseBody::Bytes(ref payload) => {
                if !response_head.contains("content-length:") {
                    response_head.push_str(&format!("content-length: {}\r\n", payload.len()));
                }
                response_head.push_str("\r\n");
                write_half.write_all(response_head.as_bytes()).await?;
                write_half.write_all(payload).await?;
            }
            ResponseBody::Stream(mut stream_body) => {
                if !response_head.contains("transfer-encoding:") {
                    response_head.push_str("transfer-encoding: chunked\r\n");
                }
                response_head.push_str("\r\n");
                write_half.write_all(response_head.as_bytes()).await?;

                let mut chunk_buf = [0u8; 8192];
                loop {
                    let count = stream_body.read(&mut chunk_buf).await?;
                    if count == 0 {
                        break;
                    }
                    let encoded = encode_chunk(&chunk_buf[..count]);
                    write_half.write_all(&encoded).await?;
                }
                write_half.write_all(terminal_chunk()).await?;
            }
        }

        write_half.flush().await?;

        // 8. If keep-alive is active, drain any remaining bytes of request body
        if will_keep_alive {
            match drainer {
                BodyDrainer::Empty => {}
                BodyDrainer::Fixed(remaining_tracker) => {
                    let remaining = remaining_tracker.load(Ordering::Relaxed);
                    if remaining > 0 {
                        let mut sink = futures_util::io::sink();
                        let mut reader = shared_reader.clone().take(remaining);
                        futures_util::io::copy(&mut reader, &mut sink).await?;
                    }
                }
                BodyDrainer::Chunked(mut chunked_drainer) => {
                    let mut sink = futures_util::io::sink();
                    futures_util::io::copy(&mut chunked_drainer, &mut sink).await?;
                }
            }
        } else {
            break;
        }
    }

    Ok(())
}

fn response_requests_close(response: &Response<ResponseBody>) -> bool {
    if let Some(val) = response.headers().get(http::header::CONNECTION) {
        if let Ok(val_str) = val.to_str() {
            return val_str.eq_ignore_ascii_case("close");
        }
    }
    false
}
