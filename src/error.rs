//! Strongly typed errors produced during HTTP/1.1 framing, parsing, and connection management.

use std::{fmt, io};

/// Errors encountered during HTTP server connection handling, decoding, or response writing.
#[derive(Debug)]
pub enum SebokError {
    /// Underlying asynchronous I/O error on the stream socket.
    Io(io::Error),
    /// Error encountered while parsing the HTTP request line or headers.
    Parse(httparse::Error),
    /// General HTTP protocol violation according to RFC 9110 / RFC 9112.
    Protocol(String),
    /// The request header block exceeded the configured size limit.
    HeaderTooLarge,
    /// The number of request headers exceeded the configured count limit.
    TooManyHeaders,
    /// The request body payload exceeded a configured limit.
    PayloadTooLarge,
    /// Connection closed prematurely by the remote peer before a message completed.
    UnexpectedEof,
}

impl SebokError {
    /// Returns the recommended HTTP status code matching this error for client rejection.
    #[must_use]
    pub fn status_code(&self) -> http::StatusCode {
        match self {
            Self::HeaderTooLarge | Self::TooManyHeaders => {
                http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
            }
            Self::PayloadTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
            Self::Parse(_) | Self::Protocol(_) => http::StatusCode::BAD_REQUEST,
            Self::Io(_) | Self::UnexpectedEof => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for SebokError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(formatter, "I/O error: {err}"),
            Self::Parse(err) => write!(formatter, "HTTP parse error: {err}"),
            Self::Protocol(msg) => write!(formatter, "HTTP protocol error: {msg}"),
            Self::HeaderTooLarge => write!(formatter, "Request header section exceeds size limit"),
            Self::TooManyHeaders => write!(formatter, "Request header count exceeds limit"),
            Self::PayloadTooLarge => write!(formatter, "Payload too large"),
            Self::UnexpectedEof => write!(formatter, "Connection closed unexpectedly by client"),
        }
    }
}

impl std::error::Error for SebokError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Parse(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for SebokError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<httparse::Error> for SebokError {
    fn from(err: httparse::Error) -> Self {
        Self::Parse(err)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn test_sebok_error_status_codes_and_display() {
        let io_err = SebokError::from(io::Error::new(io::ErrorKind::ConnectionReset, "reset"));
        assert_eq!(
            io_err.status_code(),
            http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(io_err.to_string().contains("I/O error: reset"));
        assert!(io_err.source().is_some());

        let parse_err = SebokError::from(httparse::Error::Token);
        assert_eq!(parse_err.status_code(), http::StatusCode::BAD_REQUEST);
        assert!(parse_err.to_string().contains("HTTP parse error"));
        assert!(parse_err.source().is_some());

        let proto_err = SebokError::Protocol("custom error".to_string());
        assert_eq!(proto_err.status_code(), http::StatusCode::BAD_REQUEST);
        assert_eq!(proto_err.to_string(), "HTTP protocol error: custom error");
        assert!(proto_err.source().is_none());

        let header_err = SebokError::HeaderTooLarge;
        assert_eq!(
            header_err.status_code(),
            http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );
        assert_eq!(
            header_err.to_string(),
            "Request header section exceeds size limit"
        );

        let count_err = SebokError::TooManyHeaders;
        assert_eq!(
            count_err.status_code(),
            http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );
        assert_eq!(count_err.to_string(), "Request header count exceeds limit");

        let payload_err = SebokError::PayloadTooLarge;
        assert_eq!(
            payload_err.status_code(),
            http::StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(payload_err.to_string(), "Payload too large");

        let eof_err = SebokError::UnexpectedEof;
        assert_eq!(
            eof_err.status_code(),
            http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            eof_err.to_string(),
            "Connection closed unexpectedly by client"
        );
    }
}
