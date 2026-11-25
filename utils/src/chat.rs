use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{
        AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
        BufReader,
    },
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};

use crate::{
    discovery::local_ips,
    extract_brackets,
    relay::{self, RelayAddr, RelayError},
    tls,
};

pub const INVITE_TIMEOUT: Duration = Duration::from_secs(60);
pub const MAX_MESSAGE_BYTES: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

static IN_CHAT: AtomicBool = AtomicBool::new(false);

pub fn is_in_chat() -> bool {
    IN_CHAT.load(Ordering::SeqCst)
}

pub fn set_in_chat(value: bool) {
    IN_CHAT.store(value, Ordering::SeqCst);
}

pub enum InviteSource {
    Lan(TcpStream),
    Relay {
        addr: RelayAddr,
        token: String,
        session: String,
    },
}

pub struct PendingInvite {
    pub from: String,
    pub origin: String,
    pub source: InviteSource,
    received: Instant,
}

#[derive(Clone, Default)]
pub struct Invites {
    slot: Arc<Mutex<Option<PendingInvite>>>,
}

impl Invites {
    pub fn offer(
        &self,
        from: String,
        origin: String,
        source: InviteSource,
    ) -> Result<(), InviteSource> {
        let mut slot = self.slot.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|p| p.received.elapsed() < INVITE_TIMEOUT)
        {
            return Err(source);
        }
        *slot = Some(PendingInvite {
            from,
            origin,
            source,
            received: Instant::now(),
        });
        Ok(())
    }

    pub fn peek(&self) -> Option<(String, String)> {
        let slot = self.slot.lock().unwrap();
        slot.as_ref()
            .filter(|p| p.received.elapsed() < INVITE_TIMEOUT)
            .map(|p| (p.from.clone(), p.origin.clone()))
    }

    pub fn take(&self) -> Option<PendingInvite> {
        self.slot
            .lock()
            .unwrap()
            .take()
            .filter(|p| p.received.elapsed() < INVITE_TIMEOUT)
    }
}

pub enum InviteResult<S> {
    Accepted(S, String),
    Declined,
    Busy,
    NoAnswer,
    Unreachable(String),
}

pub async fn await_answer<S>(mut stream: S) -> InviteResult<S>
where
    S: AsyncRead + Unpin,
{
    let line = match timeout(INVITE_TIMEOUT, read_line_raw(&mut stream)).await {
        Ok(Some(line)) => line,
        Ok(None) => return InviteResult::Declined,
        Err(_) => return InviteResult::NoAnswer,
    };

    if line.starts_with("ACCEPT") {
        let name = extract_brackets(&line)
            .first()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Unknown".into());
        InviteResult::Accepted(stream, name)
    } else if line.starts_with("BUSY") {
        InviteResult::Busy
    } else {
        InviteResult::Declined
    }
}

pub async fn send_invite(addr: SocketAddr, my_name: &str) -> InviteResult<TcpStream> {
    let mut stream = match timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return InviteResult::Unreachable(e.to_string()),
        Err(_) => return InviteResult::Unreachable("connect timed out".into()),
    };
    if let Err(e) = stream
        .write_all(format!("INVITE <{my_name}>\n").as_bytes())
        .await
    {
        return InviteResult::Unreachable(e.to_string());
    }
    await_answer(stream).await
}

async fn invite_with_fallback(addr: SocketAddr, my_name: &str) -> InviteResult<TcpStream> {
    let locals = local_ips();
    let mut candidates = vec![addr];
    if locals.contains(&addr.ip()) {
        candidates.extend(
            locals
                .iter()
                .filter(|ip| **ip != addr.ip())
                .map(|ip| SocketAddr::new(*ip, addr.port())),
        );
    }

    let mut last = InviteResult::Unreachable("no address to try".into());
    for candidate in candidates {
        last = send_invite(candidate, my_name).await;
        if !matches!(last, InviteResult::Unreachable(_)) {
            break;
        }
    }
    last
}

async fn invite_via_relay(
    addr: &RelayAddr,
    my_name: &str,
    to: &str,
    session: &str,
) -> InviteResult<relay::RelayStream> {
    match relay::connect(addr, my_name, to, session).await {
        Ok(stream) => await_answer(stream).await,
        Err(RelayError::Status(404)) => InviteResult::Unreachable("not online on relay".into()),
        Err(RelayError::Status(429)) => {
            InviteResult::Unreachable("too many invites, try again in a minute".into())
        }
        Err(e) => InviteResult::Unreachable(e.to_string()),
    }
}

pub async fn read_line_raw<S>(stream: &mut S) -> Option<String>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match stream.read(&mut byte).await {
            Ok(0) | Err(_) => return None,
            Ok(_) if byte[0] == b'\n' => break,
            Ok(_) => buf.push(byte[0]),
        }
        if buf.len() > 1024 {
            return None;
        }
    }
    Some(String::from_utf8_lossy(&buf).trim().to_string())
}

pub enum ChatEvent {
    Message(String),
    Closed(Option<String>),
}

pub struct ChatHandle {
    outgoing: mpsc::UnboundedSender<String>,
    events: mpsc::UnboundedReceiver<ChatEvent>,
    reader: JoinHandle<()>,
}

impl ChatHandle {
    pub fn send(&self, text: &str) -> bool {
        self.outgoing.send(text.to_string()).is_ok()
    }

    pub fn try_event(&mut self) -> Option<ChatEvent> {
        self.events.try_recv().ok()
    }
}

impl Drop for ChatHandle {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

pub struct Connected {
    pub peer: String,
    pub fingerprint: String,
    pub chat: ChatHandle,
}

async fn read_chat_line<R>(reader: &mut R) -> Result<Option<String>, String>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = Vec::new();
    let read = (&mut *reader)
        .take(MAX_MESSAGE_BYTES as u64 + 1)
        .read_until(b'\n', &mut buf)
        .await;
    match read {
        Ok(0) => Ok(None),
        Ok(_) if buf.last() == Some(&b'\n') => Ok(Some(
            String::from_utf8_lossy(&buf)
                .trim_end_matches(['\r', '\n'])
                .to_string(),
        )),
        Ok(_) if buf.len() > MAX_MESSAGE_BYTES => {
            Err("peer sent a message that is too long".into())
        }
        Ok(_) => Ok(None),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

pub fn spawn_chat<S>(stream: S) -> ChatHandle
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (rd, mut wr) = tokio::io::split(stream);
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let (ev_tx, ev_rx) = mpsc::unbounded_channel();

    let reader = tokio::spawn(async move {
        let mut reader = BufReader::new(rd);
        loop {
            let event = match read_chat_line(&mut reader).await {
                Ok(Some(line)) => match line.strip_prefix("MSG ") {
                    Some(text) => ChatEvent::Message(text.to_string()),
                    None if line == "BYE" => ChatEvent::Closed(None),
                    None => continue,
                },
                Ok(None) => ChatEvent::Closed(None),
                Err(e) => ChatEvent::Closed(Some(e)),
            };
            let closed = matches!(event, ChatEvent::Closed(_));
            if ev_tx.send(event).is_err() || closed {
                return;
            }
        }
    });

    tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            let clean: String = text.chars().filter(|c| !c.is_control()).collect();
            let sent = wr.write_all(format!("MSG {clean}\n").as_bytes()).await;
            if sent.is_err() || wr.flush().await.is_err() {
                return;
            }
        }
        let _ = wr.write_all(b"BYE\n").await;
        let _ = wr.flush().await;
        let _ = wr.shutdown().await;
    });

    ChatHandle {
        outgoing: out_tx,
        events: ev_rx,
        reader,
    }
}

async fn finish_invite<S>(result: InviteResult<S>, peer: &str) -> Result<Connected, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match result {
        InviteResult::Accepted(stream, name) => {
            let (tls, fingerprint) = tls::connect(stream)
                .await
                .map_err(|e| format!("TLS handshake with {name} failed: {e}"))?;
            Ok(Connected {
                peer: name,
                fingerprint,
                chat: spawn_chat(tls),
            })
        }
        InviteResult::Declined => Err(format!("{peer} declined.")),
        InviteResult::Busy => Err(format!("{peer} is busy in another chat.")),
        InviteResult::NoAnswer => Err(format!("{peer} didn't answer.")),
        InviteResult::Unreachable(e) => Err(format!("Couldn't reach {peer}: {e}.")),
    }
}

async fn accept_tls<S>(stream: S, peer: String) -> Result<Connected, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tls, fingerprint) = tls::accept(stream)
        .await
        .map_err(|e| format!("TLS handshake with {peer} failed: {e}"))?;
    Ok(Connected {
        peer,
        fingerprint,
        chat: spawn_chat(tls),
    })
}

pub async fn invite_lan(addr: SocketAddr, my_name: &str, peer: &str) -> Result<Connected, String> {
    finish_invite(invite_with_fallback(addr, my_name).await, peer).await
}

pub async fn invite_relay(
    relay_addr: &RelayAddr,
    my_name: &str,
    peer: &str,
    session: Option<String>,
) -> Result<Connected, String> {
    let session = session.ok_or("Not connected to the relay yet.")?;
    finish_invite(
        invite_via_relay(relay_addr, my_name, peer, &session).await,
        peer,
    )
    .await
}

pub async fn accept_invite(invite: PendingInvite, my_name: &str) -> Result<Connected, String> {
    let PendingInvite { from, source, .. } = invite;
    match source {
        InviteSource::Lan(mut stream) => {
            stream
                .write_all(format!("ACCEPT <{my_name}>\n").as_bytes())
                .await
                .map_err(|_| "The invite expired.".to_string())?;
            accept_tls(stream, from).await
        }
        InviteSource::Relay {
            addr,
            token,
            session,
        } => {
            let stream = relay::join(&addr, &token, my_name, &session)
                .await
                .map_err(|e| format!("Couldn't join {from}: {e}."))?;
            accept_tls(stream, from).await
        }
    }
}

pub async fn decline_invite(invite: PendingInvite, my_name: &str) {
    match invite.source {
        InviteSource::Lan(mut stream) => {
            let _ = stream.write_all(b"DECLINE\n").await;
        }
        InviteSource::Relay {
            addr,
            token,
            session,
        } => {
            let _ = relay::decline(&addr, &token, my_name, &session).await;
        }
    }
}
