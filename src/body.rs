//! Streaming request and response payload representations.

use std::{
    cmp, io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_io::AsyncRead;
use futures_util::AsyncReadExt;

use crate::{chunked::ChunkedDecoder, error::SebokError};

/// Internal payload stream variant for incoming HTTP request bodies.
enum RequestBodyInner {
    Empty,
    Stream(Box<dyn AsyncRead + Send + Sync + Unpin>),
}

/// Incoming HTTP request payload supporting streaming fixed-length and chunked bodies.
pub struct RequestBody {
    inner: RequestBodyInner,
}

impl RequestBody {
    /// Creates an empty request body yielding EOF immediately.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            inner: RequestBodyInner::Empty,
        }
    }

    /// Creates a request body from an asynchronous reader stream.
    pub fn from_reader<R>(reader: R) -> Self
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        Self {
            inner: RequestBodyInner::Stream(Box::new(reader)),
        }
    }

    /// Reads the entire request body into an in-memory [`Bytes`] buffer.
    pub async fn into_bytes(mut self) -> Result<Bytes, SebokError> {
        let mut buffer = Vec::new();
        self.read_to_end(&mut buffer).await?;
        Ok(Bytes::from(buffer))
    }

    /// Drains and discards any remaining unread bytes from the request body stream.
    ///
    /// Essential for persistent Keep-Alive connections to preserve socket framing alignment.
    pub async fn drain(mut self) -> Result<u64, SebokError> {
        let mut sink = futures_util::io::sink();
        let copied = futures_util::io::copy(&mut self, &mut sink).await?;
        Ok(copied)
    }
}

impl AsyncRead for RequestBody {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.inner {
            RequestBodyInner::Empty => Poll::Ready(Ok(0)),
            RequestBodyInner::Stream(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

/// Reader that limits reads to a maximum length while tracking remaining unread bytes atomically.
pub struct TrackedTake<R> {
    inner: R,
    remaining: Arc<AtomicU64>,
}

impl<R> TrackedTake<R> {
    /// Creates a new `TrackedTake` reader and returns both the reader and the atomic remaining tracker.
    pub fn new(inner: R, limit: u64) -> (Self, Arc<AtomicU64>) {
        let remaining = Arc::new(AtomicU64::new(limit));
        (
            Self {
                inner,
                remaining: remaining.clone(),
            },
            remaining,
        )
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TrackedTake<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let current = self.remaining.load(Ordering::Relaxed);
        if current == 0 {
            return Poll::Ready(Ok(0));
        }

        let max_to_read = cmp::min(buf.len() as u64, current) as usize;
        match Pin::new(&mut self.inner).poll_read(cx, &mut buf[..max_to_read]) {
            Poll::Ready(Ok(bytes_read)) => {
                self.remaining
                    .fetch_sub(bytes_read as u64, Ordering::Relaxed);
                Poll::Ready(Ok(bytes_read))
            }
            other => other,
        }
    }
}

/// Shared chunked decoder allowing both the service and the server drainer to read the same stream.
#[derive(Clone)]
pub struct SharedChunkedDecoder<R> {
    inner: Arc<Mutex<ChunkedDecoder<R>>>,
}

impl<R: AsyncRead + Unpin> SharedChunkedDecoder<R> {
    /// Creates a new shared chunked decoder wrapping the reader.
    pub fn new(reader: R) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ChunkedDecoder::new(reader))),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for SharedChunkedDecoder<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Pin::new(&mut *guard).poll_read(cx, buf)
    }
}

/// Outgoing HTTP response payload supporting empty, buffered, and indefinite streaming bodies.
#[derive(Default)]
pub enum ResponseBody {
    /// Zero-length response payload.
    #[default]
    Empty,
    /// In-memory fixed-length byte buffer with known `Content-Length`.
    Bytes(Bytes),
    /// Streaming body framed via RFC 9112 chunked transfer encoding.
    Stream(Box<dyn AsyncRead + Send + Sync + Unpin>),
}

impl ResponseBody {
    /// Creates a streaming response body framed via chunked transfer encoding.
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: AsyncRead + Send + Sync + Unpin + 'static,
    {
        Self::Stream(Box::new(stream))
    }

    /// Returns `true` if this response body contains no data.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Bytes(bytes) => bytes.is_empty(),
            Self::Stream(_) => false,
        }
    }

    /// Returns the exact byte length if known in advance.
    #[must_use]
    pub fn len(&self) -> Option<usize> {
        match self {
            Self::Empty => Some(0),
            Self::Bytes(bytes) => Some(bytes.len()),
            Self::Stream(_) => None,
        }
    }
}

impl From<()> for ResponseBody {
    fn from((): ()) -> Self {
        Self::Empty
    }
}

impl From<Bytes> for ResponseBody {
    fn from(bytes: Bytes) -> Self {
        Self::Bytes(bytes)
    }
}

impl From<Vec<u8>> for ResponseBody {
    fn from(vec: Vec<u8>) -> Self {
        Self::Bytes(Bytes::from(vec))
    }
}

impl From<&'static [u8]> for ResponseBody {
    fn from(slice: &'static [u8]) -> Self {
        Self::Bytes(Bytes::from_static(slice))
    }
}

impl From<String> for ResponseBody {
    fn from(text: String) -> Self {
        Self::Bytes(Bytes::from(text.into_bytes()))
    }
}

impl From<&'static str> for ResponseBody {
    fn from(text: &'static str) -> Self {
        Self::Bytes(Bytes::from_static(text.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_response_body_conversions_and_metadata() {
        let empty = ResponseBody::from(());
        assert!(empty.is_empty());
        assert_eq!(empty.len(), Some(0));

        let from_str = ResponseBody::from("hello static");
        assert!(!from_str.is_empty());
        assert_eq!(from_str.len(), Some(12));

        let from_string = ResponseBody::from("hello owned".to_string());
        assert!(!from_string.is_empty());
        assert_eq!(from_string.len(), Some(11));

        let from_vec = ResponseBody::from(vec![1, 2, 3]);
        assert_eq!(from_vec.len(), Some(3));

        let from_slice = ResponseBody::from(b"slice" as &'static [u8]);
        assert_eq!(from_slice.len(), Some(5));

        let from_bytes = ResponseBody::from(Bytes::from_static(b"bytes"));
        assert_eq!(from_bytes.len(), Some(5));

        let stream_body = ResponseBody::from_stream(futures_util::io::Cursor::new(vec![1, 2]));
        assert!(!stream_body.is_empty());
        assert_eq!(stream_body.len(), None);
    }

    #[tokio::test]
    async fn test_request_body_drain() {
        let cursor = futures_util::io::Cursor::new(vec![1, 2, 3, 4, 5]);
        let body = RequestBody::from_reader(cursor);
        let drained = body.drain().await.unwrap();
        assert_eq!(drained, 5);
    }
}
