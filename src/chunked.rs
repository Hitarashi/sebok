//! Chunked transfer encoding and decoding complying with RFC 9112 Section 7.1.

use std::{
    cmp,
    io::{self, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};

use futures_io::AsyncRead;

#[derive(Debug, PartialEq, Eq)]
enum DecodeState {
    ReadSize,
    ReadData(u64),
    ReadCrLfAfterData,
    ReadTrailers,
    Done,
}

/// Asynchronous reader stream that decodes RFC 9112 chunked transfer encoding on the fly.
pub struct ChunkedDecoder<R> {
    reader: R,
    state: DecodeState,
    buffer: Vec<u8>,
}

impl<R: AsyncRead + Unpin> ChunkedDecoder<R> {
    /// Creates a new `ChunkedDecoder` wrapping the underlying stream.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            state: DecodeState::ReadSize,
            buffer: Vec::with_capacity(64),
        }
    }

    fn parse_hex_size(slice: &[u8]) -> Result<u64, io::Error> {
        let line = std::str::from_utf8(slice)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err.to_string()))?;
        // Ignore chunk extensions after semicolon if present
        let hex_str = line.split(';').next().unwrap_or("").trim();
        u64::from_str_radix(hex_str, 16)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err.to_string()))
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ChunkedDecoder<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.state {
                DecodeState::ReadSize => {
                    let mut byte = [0u8; 1];
                    let mut pin_reader = Pin::new(&mut self.reader);
                    match pin_reader.as_mut().poll_read(cx, &mut byte) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(io::Error::new(
                                ErrorKind::UnexpectedEof,
                                "Unexpected EOF while reading chunk size",
                            )));
                        }
                        Poll::Ready(Ok(1)) => {
                            self.buffer.push(byte[0]);
                            if self.buffer.len() >= 2
                                && self.buffer[self.buffer.len() - 2..] == *b"\r\n"
                            {
                                let size_slice = &self.buffer[..self.buffer.len() - 2];
                                let chunk_size = Self::parse_hex_size(size_slice)?;
                                self.buffer.clear();

                                if chunk_size == 0 {
                                    self.state = DecodeState::ReadTrailers;
                                } else {
                                    self.state = DecodeState::ReadData(chunk_size);
                                }
                            } else if self.buffer.len() > 1024 {
                                return Poll::Ready(Err(io::Error::new(
                                    ErrorKind::InvalidData,
                                    "Chunk size line exceeded maximum length",
                                )));
                            }
                        }
                        Poll::Ready(Ok(_)) => unreachable!(),
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                DecodeState::ReadData(remaining) => {
                    let max_to_read = cmp::min(buf.len() as u64, remaining) as usize;
                    let mut pin_reader = Pin::new(&mut self.reader);
                    match pin_reader.as_mut().poll_read(cx, &mut buf[..max_to_read]) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(io::Error::new(
                                ErrorKind::UnexpectedEof,
                                "Unexpected EOF while reading chunk data",
                            )));
                        }
                        Poll::Ready(Ok(bytes_read)) => {
                            let new_remaining = remaining - bytes_read as u64;
                            if new_remaining == 0 {
                                self.state = DecodeState::ReadCrLfAfterData;
                            } else {
                                self.state = DecodeState::ReadData(new_remaining);
                            }
                            return Poll::Ready(Ok(bytes_read));
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                DecodeState::ReadCrLfAfterData => {
                    let mut crlf = [0u8; 1];
                    let mut pin_reader = Pin::new(&mut self.reader);
                    match pin_reader.as_mut().poll_read(cx, &mut crlf) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(io::Error::new(
                                ErrorKind::UnexpectedEof,
                                "Unexpected EOF while reading chunk trailer CRLF",
                            )));
                        }
                        Poll::Ready(Ok(1)) => {
                            self.buffer.push(crlf[0]);
                            if self.buffer.len() == 2 {
                                if self.buffer == *b"\r\n" {
                                    self.buffer.clear();
                                    self.state = DecodeState::ReadSize;
                                } else {
                                    return Poll::Ready(Err(io::Error::new(
                                        ErrorKind::InvalidData,
                                        "Expected CRLF following chunk data",
                                    )));
                                }
                            }
                        }
                        Poll::Ready(Ok(_)) => unreachable!(),
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                DecodeState::ReadTrailers => {
                    let mut byte = [0u8; 1];
                    let mut pin_reader = Pin::new(&mut self.reader);
                    match pin_reader.as_mut().poll_read(cx, &mut byte) {
                        Poll::Ready(Ok(0)) => {
                            self.state = DecodeState::Done;
                            return Poll::Ready(Ok(0));
                        }
                        Poll::Ready(Ok(1)) => {
                            self.buffer.push(byte[0]);
                            if self.buffer.len() >= 2
                                && self.buffer[self.buffer.len() - 2..] == *b"\r\n"
                            {
                                if self.buffer.len() == 2 {
                                    // Empty line signifies end of trailers
                                    self.state = DecodeState::Done;
                                    return Poll::Ready(Ok(0));
                                }
                                self.buffer.clear();
                            } else if self.buffer.len() > 8192 {
                                return Poll::Ready(Err(io::Error::new(
                                    ErrorKind::InvalidData,
                                    "Trailer section exceeded maximum allowed length",
                                )));
                            }
                        }
                        Poll::Ready(Ok(_)) => unreachable!(),
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                DecodeState::Done => {
                    return Poll::Ready(Ok(0));
                }
            }
        }
    }
}

/// Encodes a slice of data as an RFC 9112 chunk.
#[must_use]
pub fn encode_chunk(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return b"0\r\n\r\n".to_vec();
    }
    let hex_len = format!("{:X}\r\n", data.len());
    let mut chunk = Vec::with_capacity(hex_len.len() + data.len() + 2);
    chunk.extend_from_slice(hex_len.as_bytes());
    chunk.extend_from_slice(data);
    chunk.extend_from_slice(b"\r\n");
    chunk
}

/// Returns the terminating RFC 9112 zero-length chunk.
#[must_use]
pub fn terminal_chunk() -> &'static [u8] {
    b"0\r\n\r\n"
}

#[cfg(test)]
mod tests {
    use futures_util::AsyncReadExt;

    use super::*;

    #[test]
    fn test_encode_chunk_and_terminal() {
        assert_eq!(encode_chunk(b""), b"0\r\n\r\n");
        assert_eq!(encode_chunk(b"test"), b"4\r\ntest\r\n");
        assert_eq!(terminal_chunk(), b"0\r\n\r\n");
    }

    #[test]
    fn test_parse_hex_size_with_extensions() {
        assert_eq!(
            ChunkedDecoder::<futures_util::io::Empty>::parse_hex_size(b"1A;ext=val").unwrap(),
            26
        );
        assert_eq!(
            ChunkedDecoder::<futures_util::io::Empty>::parse_hex_size(b"10").unwrap(),
            16
        );
        assert!(ChunkedDecoder::<futures_util::io::Empty>::parse_hex_size(b"invalid_hex").is_err());
    }

    #[tokio::test]
    async fn test_chunked_decoder_with_trailers() {
        let chunked_data = b"5\r\nhello\r\n0\r\nX-Trailer: val\r\n\r\n";
        let mut decoder = ChunkedDecoder::new(futures_util::io::Cursor::new(chunked_data));
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello");
    }
}
