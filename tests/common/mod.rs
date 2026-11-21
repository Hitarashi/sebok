//! In-memory asynchronous test duplex stream and utilities for Sebok test suites.

use std::{
    cmp, io,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll, Waker},
};

use futures_io::{AsyncRead, AsyncWrite};

#[derive(Default)]
struct PipeBufferInner {
    data: Vec<u8>,
    cursor: usize,
    waker: Option<Waker>,
    closed: bool,
}

#[derive(Default, Clone)]
struct PipeBuffer(Arc<RwLock<PipeBufferInner>>);

impl PipeBuffer {
    fn close(&self) {
        let mut inner = self.0.write().unwrap();
        inner.closed = true;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }

    fn read_all_bytes(&self) -> Vec<u8> {
        let inner = self.0.read().unwrap();
        inner.data.clone()
    }
}

impl AsyncRead for PipeBuffer {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.0.write().unwrap();
        if inner.cursor < inner.data.len() {
            let available = inner.data.len() - inner.cursor;
            let to_copy = cmp::min(buf.len(), available);
            buf[..to_copy].copy_from_slice(&inner.data[inner.cursor..inner.cursor + to_copy]);
            inner.cursor += to_copy;
            Poll::Ready(Ok(to_copy))
        } else if inner.closed {
            Poll::Ready(Ok(0))
        } else {
            inner.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl AsyncWrite for PipeBuffer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let _ = cx;
        let mut inner = self.0.write().unwrap();
        inner.data.extend_from_slice(buf);
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = cx;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = cx;
        self.close();
        Poll::Ready(Ok(()))
    }
}

/// Asynchronous duplex endpoint for in-memory connection testing.
pub struct DuplexStream {
    reader: PipeBuffer,
    writer: PipeBuffer,
}

impl AsyncRead for DuplexStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for DuplexStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_close(cx)
    }
}

/// Creates a pair of connected in-memory duplex streams `(client, server)`.
pub fn create_duplex() -> (DuplexStream, DuplexStream) {
    let client_to_server = PipeBuffer::default();
    let server_to_client = PipeBuffer::default();

    let client = DuplexStream {
        reader: server_to_client.clone(),
        writer: client_to_server.clone(),
    };

    let server = DuplexStream {
        reader: client_to_server,
        writer: server_to_client,
    };

    (client, server)
}

/// Helper returning all raw bytes received by the given duplex stream's reader.
pub fn stream_output(stream: &DuplexStream) -> Vec<u8> {
    stream.reader.read_all_bytes()
}
