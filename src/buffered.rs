//! Buffered connection reader preserving unread bytes across persistent Keep-Alive request cycles.

use std::{
    cmp,
    io::{self, ErrorKind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_lock::Mutex;
use futures_io::AsyncRead;
use futures_util::AsyncReadExt;

use crate::error::SebokError;

/// Internal buffered stream over an asynchronous read half.
pub struct BufferedStream<R> {
    inner: R,
    buffer: Vec<u8>,
    cursor: usize,
}

impl<R: AsyncRead + Unpin> BufferedStream<R> {
    /// Creates a new `BufferedStream` wrapping the stream with an initial 4096-byte capacity.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buffer: Vec::with_capacity(4096),
            cursor: 0,
        }
    }

    /// Reads from the underlying stream until `\r\n\r\n` is found in the buffer.
    ///
    /// Compacts previously consumed bytes, then repeatedly reads until the delimiter is reached.
    pub async fn read_headers(
        &mut self,
        max_header_size: usize,
    ) -> Result<Option<usize>, SebokError> {
        if self.cursor > 0 {
            self.buffer.drain(..self.cursor);
            self.cursor = 0;
        }

        let mut temp = [0u8; 1024];
        loop {
            if let Some(pos) = find_crlf_crlf(&self.buffer) {
                let header_end = pos + 4;
                if header_end > max_header_size {
                    return Err(SebokError::HeaderTooLarge);
                }
                return Ok(Some(header_end));
            }

            if self.buffer.len() >= max_header_size {
                return Err(SebokError::HeaderTooLarge);
            }

            let bytes_read = self.inner.read(&mut temp).await?;
            if bytes_read == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err(SebokError::UnexpectedEof);
            }

            self.buffer.extend_from_slice(&temp[..bytes_read]);
        }
    }

    /// Returns a slice of the headers up to `header_end`.
    #[must_use]
    pub fn headers_slice(&self, header_end: usize) -> &[u8] {
        &self.buffer[..header_end]
    }

    /// Advances the cursor past the header boundary, preparing the stream for body reading.
    pub fn advance_cursor(&mut self, header_end: usize) {
        self.cursor = header_end;
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BufferedStream<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.cursor < self.buffer.len() {
            let available = self.buffer.len() - self.cursor;
            let to_read = cmp::min(buf.len(), available);
            buf[..to_read].copy_from_slice(&self.buffer[self.cursor..self.cursor + to_read]);
            self.cursor += to_read;
            if self.cursor == self.buffer.len() {
                self.buffer.clear();
                self.cursor = 0;
            }
            return Poll::Ready(Ok(to_read));
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

fn find_crlf_crlf(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 4 {
        return None;
    }
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Shared, cloneable reader handle wrapping `BufferedStream` in an asynchronous mutex.
pub struct SharedReader<R> {
    inner: Arc<Mutex<BufferedStream<R>>>,
}

impl<R> Clone for SharedReader<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R: AsyncRead + Unpin> SharedReader<R> {
    /// Creates a new shared reader.
    pub fn new(stream: R) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BufferedStream::new(stream))),
        }
    }

    /// Acquires the lock asynchronously to read headers or access the stream directly.
    pub async fn lock(&self) -> async_lock::MutexGuard<'_, BufferedStream<R>> {
        self.inner.lock().await
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncRead for SharedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.inner.try_lock() {
            Some(mut guard) => Pin::new(&mut *guard).poll_read(cx, buf),
            None => Poll::Ready(Err(io::Error::new(
                ErrorKind::WouldBlock,
                "Concurrent read on connection stream",
            ))),
        }
    }
}
