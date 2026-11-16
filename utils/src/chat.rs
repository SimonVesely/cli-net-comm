use std::{
    io::Write,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};

use crate::{
    Config, OnlineList,
    discovery::local_ips,
    extract_brackets,
    relay::{self, RelayAddr, RelayError, RelayState},
    return_input, tls,
};

pub const INVITE_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

static IN_CHAT: AtomicBool = AtomicBool::new(false);

pub fn is_in_chat() -> bool {
    IN_CHAT.load(Ordering::SeqCst)
}

fn set_in_chat(value: bool) {
    IN_CHAT.store(value, Ordering::SeqCst);
}

pub enum InviteSource {
    Lan(TcpStream),
    Relay { addr: RelayAddr, id: u64 },
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
) -> InviteResult<relay::RelayStream> {
    match relay::connect(addr, my_name, to).await {
        Ok(stream) => await_answer(stream).await,
        Err(RelayError::Status(404)) => InviteResult::Unreachable("not online on relay".into()),
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

fn read_line_blocking() -> String {
    tokio::task::block_in_place(return_input)
}

fn flush() {
    let _ = std::io::stdout().flush();
}

fn pause(msg: &str) {
    println!("{msg} Press Enter to go back.");
    read_line_blocking();
}

pub async fn chat_session<S>(stream: S, peer: String, fingerprint: String)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    set_in_chat(true);
    print!("\x1b[2J\x1b[H");
    println!("── Chat with {peer} ── (TLS 1.3, type /quit to leave)");
    println!("Fingerprint: {fingerprint}  (must match on {peer}'s screen)\n");

    let (rd, mut wr) = tokio::io::split(stream);
    let peer_gone = Arc::new(AtomicBool::new(false));

    let reader = {
        let peer = peer.clone();
        let peer_gone = peer_gone.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(rd).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(text) = line.strip_prefix("MSG ") {
                    print!("\r\x1b[2K{peer}: {text}\n> ");
                    flush();
                } else if line == "BYE" {
                    break;
                }
            }
            peer_gone.store(true, Ordering::SeqCst);
            print!("\r\x1b[2K[{peer} left the chat — press Enter]\n");
            flush();
        })
    };

    loop {
        print!("> ");
        flush();
        let input = read_line_blocking();
        if peer_gone.load(Ordering::SeqCst) {
            break;
        }
        let text = input.trim_end_matches(['\r', '\n']);
        if text == "/quit" {
            let _ = wr.write_all(b"BYE\n").await;
            let _ = wr.flush().await;
            break;
        }
        if text.trim().is_empty() {
            continue;
        }
        print!("\x1b[1A\r\x1b[2Kyou: {text}\n");
        let sent = wr.write_all(format!("MSG {text}\n").as_bytes()).await;
        if sent.is_err() || wr.flush().await.is_err() {
            println!("[connection lost — press Enter]");
            read_line_blocking();
            break;
        }
    }

    let _ = wr.shutdown().await;
    reader.abort();
    set_in_chat(false);
}

async fn finish_invite<S>(result: InviteResult<S>, peer: &str)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let msg = match result {
        InviteResult::Accepted(stream, name) => match tls::connect(stream).await {
            Ok((tls, fingerprint)) => return chat_session(tls, name, fingerprint).await,
            Err(e) => format!("TLS handshake with {name} failed: {e}."),
        },
        InviteResult::Declined => format!("{peer} declined."),
        InviteResult::Busy => format!("{peer} is busy in another chat."),
        InviteResult::NoAnswer => format!("{peer} didn't answer."),
        InviteResult::Unreachable(e) => format!("Couldn't reach {peer}: {e}."),
    };
    pause(&msg);
}

async fn accept_and_chat<S>(stream: S, peer: String)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match tls::accept(stream).await {
        Ok((tls, fingerprint)) => chat_session(tls, peer, fingerprint).await,
        Err(e) => {
            print!("\x1b[2J\x1b[H");
            pause(&format!("TLS handshake with {peer} failed: {e}."));
        }
    }
}

enum Target {
    Lan(SocketAddr, String),
    Relay(RelayAddr, String),
}

pub async fn active_connection(config: &Config, online: &OnlineList, relay_state: &RelayState) {
    let mut targets: Vec<Target> = online
        .snapshot()
        .into_iter()
        .map(|(addr, peer)| Target::Lan(addr, peer.username))
        .collect();
    if let Some(relay_addr) = &config.relay {
        targets.extend(
            relay_state
                .users()
                .into_iter()
                .map(|name| Target::Relay(relay_addr.clone(), name)),
        );
    }

    print!("\x1b[2J\x1b[H");
    if targets.is_empty() {
        pause("Nobody online yet.");
        return;
    }

    println!("Who do you want to chat with? (Enter = back)\n");
    for (i, target) in targets.iter().enumerate() {
        match target {
            Target::Lan(addr, name) => println!("{} -> {:<16} {}", i + 1, name, addr),
            Target::Relay(_, name) => println!("{} -> {:<16} [relay]", i + 1, name),
        }
    }
    print!("\n> ");
    flush();

    let choice = match read_line_blocking().trim().parse::<usize>() {
        Ok(n) if (1..=targets.len()).contains(&n) => n,
        _ => return,
    };

    set_in_chat(true);
    match &targets[choice - 1] {
        Target::Lan(addr, name) => {
            println!(
                "Inviting {name} at {addr}… waiting up to {}s for them to accept.",
                INVITE_TIMEOUT.as_secs()
            );
            flush();
            let result = invite_with_fallback(*addr, &config.username).await;
            set_in_chat(false);
            finish_invite(result, name).await;
        }
        Target::Relay(relay_addr, name) => {
            println!(
                "Inviting {name} via relay… waiting up to {}s for them to accept.",
                INVITE_TIMEOUT.as_secs()
            );
            flush();
            let result = invite_via_relay(relay_addr, &config.username, name).await;
            set_in_chat(false);
            finish_invite(result, name).await;
        }
    }
}

pub async fn handle_pending_invite(config: &Config, invites: &Invites, accept: bool) {
    let Some(invite) = invites.take() else {
        return;
    };
    let PendingInvite { from, source, .. } = invite;

    match source {
        InviteSource::Lan(mut stream) => {
            let reply = if accept {
                format!("ACCEPT <{}>\n", config.username)
            } else {
                "DECLINE\n".to_string()
            };
            let sent = stream.write_all(reply.as_bytes()).await.is_ok();
            if accept && sent {
                accept_and_chat(stream, from).await;
            } else if accept {
                print!("\x1b[2J\x1b[H");
                pause("The invite expired.");
            }
        }
        InviteSource::Relay { addr, id } if accept => {
            print!("\x1b[2J\x1b[H");
            println!("Joining {from} via relay…");
            flush();
            match relay::join(&addr, id, &config.username).await {
                Ok(stream) => accept_and_chat(stream, from).await,
                Err(e) => pause(&format!("Couldn't join {from}: {e}.")),
            }
        }
        InviteSource::Relay { addr, id } => {
            let name = config.username.clone();
            tokio::spawn(async move {
                let _ = relay::decline(&addr, id, &name).await;
            });
        }
    }
}
