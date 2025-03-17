use super::sync::RawPacketStream as SyncRawPacketStream;
pub use super::sync::{Filter, FilterProgram};
use futures_lite::ready;
use std::io::{Read, Result, Write};
use std::os::unix::prelude::{AsRawFd, FromRawFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Debug)]
pub struct RawPacketStreamTx(Arc<AsyncFd<SyncRawPacketStream>>);
#[derive(Debug)]
pub struct RawPacketStreamRx(Arc<AsyncFd<SyncRawPacketStream>>);
#[derive(Debug)]
pub struct RawPacketStream(AsyncFd<SyncRawPacketStream>);

impl AsyncRead for RawPacketStreamRx {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        loop {
            let mut guard = ready!(self.0.poll_read_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().read(buf.initialize_unfilled())) {
                Ok(result) => {
                    buf.advance(result?);
                    return Poll::Ready(Ok(()));
                }
                Err(_would_block) => continue,
            }
        }
    }
}

impl<'a> AsyncRead for &'a RawPacketStreamRx {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        loop {
            let mut guard = ready!(self.0.poll_read_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().read(buf.initialize_unfilled())) {
                Ok(result) => {
                    buf.advance(result?);
                    return Poll::Ready(Ok(()));
                }
                Err(_would_block) => continue,
            }
        }
    }
}

impl AsyncWrite for RawPacketStreamTx {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        loop {
            let mut guard = ready!(self.0.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().write(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<'a> AsyncWrite for &'a RawPacketStreamTx {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        loop {
            let mut guard = ready!(self.0.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().write(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl RawPacketStream {
    pub fn new() -> Result<RawPacketStream> {
        Ok(SyncRawPacketStream::new()?.into())
    }

    pub fn bind(&mut self, name: &str) -> Result<()> {
        self.0.get_ref().bind_internal(name)
    }

    pub fn set_promisc(&mut self, name: &str, state: bool) -> Result<()> {
        self.0.get_ref().set_promisc_internal(name, state)
    }

    pub fn set_bpf_filter(&mut self, filter: FilterProgram) -> Result<()> {
        self.0.get_ref().set_bpf_filter_internal(filter)
    }

    pub fn drain(&mut self) {
        self.0.get_ref().drain_internal()
    }

    pub fn into_split(self) -> (RawPacketStreamRx, RawPacketStreamTx) {
        // get the original sync stream
        let arc = Arc::new(self.0);
        (RawPacketStreamRx(arc.clone()), RawPacketStreamTx(arc))
    }
}

impl AsyncRead for RawPacketStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        loop {
            let mut guard = ready!(self.0.poll_read_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().read(buf.initialize_unfilled())) {
                Ok(result) => {
                    buf.advance(result?);
                    return Poll::Ready(Ok(()));
                }
                Err(_would_block) => continue,
            }
        }
    }
}

impl<'a> AsyncRead for &'a RawPacketStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        loop {
            let mut guard = ready!(self.0.poll_read_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().read(buf.initialize_unfilled())) {
                Ok(result) => {
                    buf.advance(result?);
                    return Poll::Ready(Ok(()));
                }
                Err(_would_block) => continue,
            }
        }
    }
}

impl AsyncWrite for RawPacketStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        loop {
            let mut guard = ready!(self.0.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().write(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<'a> AsyncWrite for &'a RawPacketStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        loop {
            let mut guard = ready!(self.0.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().write(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl From<SyncRawPacketStream> for RawPacketStream {
    fn from(mut sync: SyncRawPacketStream) -> RawPacketStream {
        sync.set_non_blocking().expect("could not set non-blocking");
        RawPacketStream(AsyncFd::new(sync).expect("oopsie whoopsie"))
    }
}

impl AsRawFd for RawPacketStream {
    fn as_raw_fd(&self) -> RawFd {
        self.0.get_ref().as_raw_fd()
    }
}

impl FromRawFd for RawPacketStream {
    unsafe fn from_raw_fd(fd: RawFd) -> RawPacketStream {
        SyncRawPacketStream::from_raw_fd(fd).into()
    }
}
