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
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};

use crate::{Config, OnlineList, discovery::local_ips, extract_brackets, return_input, tls};

pub const INVITE_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

static IN_CHAT: AtomicBool = AtomicBool::new(false);

pub fn is_in_chat() -> bool {
    IN_CHAT.load(Ordering::SeqCst)
}

fn set_in_chat(value: bool) {
    IN_CHAT.store(value, Ordering::SeqCst);
}

pub struct PendingInvite {
    pub from: String,
    pub addr: SocketAddr,
    pub stream: TcpStream,
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
        addr: SocketAddr,
        stream: TcpStream,
    ) -> Result<(), TcpStream> {
        let mut slot = self.slot.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|p| p.received.elapsed() < INVITE_TIMEOUT)
        {
            return Err(stream);
        }
        *slot = Some(PendingInvite {
            from,
            addr,
            stream,
            received: Instant::now(),
        });
        Ok(())
    }

    pub fn peek(&self) -> Option<(String, SocketAddr)> {
        let slot = self.slot.lock().unwrap();
        slot.as_ref()
            .filter(|p| p.received.elapsed() < INVITE_TIMEOUT)
            .map(|p| (p.from.clone(), p.addr))
    }

    pub fn take(&self) -> Option<PendingInvite> {
        self.slot
            .lock()
            .unwrap()
            .take()
            .filter(|p| p.received.elapsed() < INVITE_TIMEOUT)
    }
}

pub enum InviteResult {
    Accepted(TcpStream, String),
    Declined,
    Busy,
    NoAnswer,
    Unreachable(String),
}

pub async fn send_invite(addr: SocketAddr, my_name: &str) -> InviteResult {
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

async fn invite_with_fallback(addr: SocketAddr, my_name: &str) -> InviteResult {
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

async fn read_line_raw(stream: &mut TcpStream) -> Option<String> {
    use tokio::io::AsyncReadExt;
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

pub async fn answer_invite(
    invite: PendingInvite,
    my_name: &str,
    accept: bool,
) -> Option<(TcpStream, String)> {
    let mut stream = invite.stream;
    let reply = if accept {
        format!("ACCEPT <{my_name}>\n")
    } else {
        "DECLINE\n".to_string()
    };
    let sent = stream.write_all(reply.as_bytes()).await.is_ok();
    (accept && sent).then_some((stream, invite.from))
}

fn read_line_blocking() -> String {
    tokio::task::block_in_place(return_input)
}

fn flush() {
    let _ = std::io::stdout().flush();
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

pub async fn active_connection(config: &Config, online: &OnlineList) {
    let peers = online.snapshot();

    print!("\x1b[2J\x1b[H");
    if peers.is_empty() {
        println!("Nobody online yet. Press Enter to go back.");
        read_line_blocking();
        return;
    }

    println!("Who do you want to chat with? (Enter = back)\n");
    for (i, (addr, peer)) in peers.iter().enumerate() {
        println!("{} -> {:<16} {}", i + 1, peer.username, addr);
    }
    print!("\n> ");
    flush();

    let choice = match read_line_blocking().trim().parse::<usize>() {
        Ok(n) if (1..=peers.len()).contains(&n) => n,
        _ => return,
    };
    let (addr, peer) = &peers[choice - 1];

    set_in_chat(true);
    println!(
        "Inviting {} at {}… waiting up to {}s for them to accept.",
        peer.username,
        addr,
        INVITE_TIMEOUT.as_secs()
    );
    flush();
    let result = invite_with_fallback(*addr, &config.username).await;
    set_in_chat(false);

    let msg = match result {
        InviteResult::Accepted(stream, name) => match tls::connect(stream).await {
            Ok((tls, fingerprint)) => return chat_session(tls, name, fingerprint).await,
            Err(e) => format!("TLS handshake with {name} failed: {e}"),
        },
        InviteResult::Declined => format!("{} declined.", peer.username),
        InviteResult::Busy => format!("{} is busy in another chat.", peer.username),
        InviteResult::NoAnswer => format!("{} didn't answer.", peer.username),
        InviteResult::Unreachable(e) => format!("Couldn't reach {}: {e}", peer.username),
    };
    println!("{msg} Press Enter to go back.");
    read_line_blocking();
}

pub async fn handle_pending_invite(config: &Config, invites: &Invites, accept: bool) {
    let Some(invite) = invites.take() else {
        return;
    };
    match answer_invite(invite, &config.username, accept).await {
        Some((stream, name)) => match tls::accept(stream).await {
            Ok((tls, fingerprint)) => chat_session(tls, name, fingerprint).await,
            Err(e) => {
                println!(
                    "\x1b[2J\x1b[HTLS handshake with {name} failed: {e}. Press Enter to go back."
                );
                read_line_blocking();
            }
        },
        None if accept => {
            println!("\x1b[2J\x1b[HThe invite expired. Press Enter to go back.");
            read_line_blocking();
        }
        None => {}
    }
}
