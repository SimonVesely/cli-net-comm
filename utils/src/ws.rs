use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Bytes, Message},
};

pub struct WsBytes<S> {
    ws: WebSocketStream<S>,
    pending: Bytes,
}

impl<S> WsBytes<S> {
    pub fn new(ws: WebSocketStream<S>) -> Self {
        Self {
            ws,
            pending: Bytes::new(),
        }
    }
}

fn to_io(e: tokio_tungstenite::tungstenite::Error) -> io::Error {
    io::Error::other(e)
}

impl<S> AsyncRead for WsBytes<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        while self.pending.is_empty() {
            match ready!(Pin::new(&mut self.ws).poll_next(cx)) {
                Some(Ok(Message::Binary(data))) => self.pending = data,
                Some(Ok(Message::Text(text))) => self.pending = Bytes::from(text),
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(())),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Poll::Ready(Err(to_io(e))),
            }
        }
        let n = self.pending.len().min(buf.remaining());
        let chunk = self.pending.split_to(n);
        buf.put_slice(&chunk);
        Poll::Ready(Ok(()))
    }
}

impl<S> AsyncWrite for WsBytes<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(Pin::new(&mut self.ws).poll_ready(cx)).map_err(to_io)?;
        Pin::new(&mut self.ws)
            .start_send(Message::Binary(Bytes::copy_from_slice(buf)))
            .map_err(to_io)?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.ws).poll_flush(cx).map_err(to_io)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.ws).poll_close(cx).map_err(to_io)
    }
}
