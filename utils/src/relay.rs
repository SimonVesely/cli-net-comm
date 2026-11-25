use std::{
    fmt, io,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use rustls::{ClientConfig, RootCertStore, crypto::ring as ring_provider, pki_types::ServerName};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
    time::{Instant, interval_at, sleep, timeout},
};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{Bytes, Error as WsError, Message, client::IntoClientRequest, http::HeaderValue},
};

use crate::{
    Config, OnlineList,
    chat::{InviteSource, Invites, is_in_chat},
    extract_brackets,
    ws::WsBytes,
};

pub const DEFAULT_RELAY: &str = "https://cli-net-comm-relay.onrender.com";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(90);
const HTTP_TIMEOUT: Duration = Duration::from_secs(90);
const KEEPALIVE: Duration = Duration::from_secs(25);
const MAX_HTTP_RESPONSE: u64 = 64 * 1024;
const SESSION_HEADER: &str = "x-session";
const MAX_BACKOFF: Duration = Duration::from_secs(60);

pub type RelayStream = WsBytes<MaybeTlsStream<TcpStream>>;
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayAddr {
    secure: bool,
    host: String,
    port: u16,
}

impl RelayAddr {
    pub fn parse(input: &str) -> Option<Self> {
        let s = input.trim().trim_end_matches('/');
        let (secure, rest) = if let Some(r) = s.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("wss://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("http://") {
            (false, r)
        } else if let Some(r) = s.strip_prefix("ws://") {
            (false, r)
        } else {
            (true, s)
        };
        if rest.is_empty() || rest.contains(['/', '?', '#', ' ']) {
            return None;
        }
        let (host, port) = match rest.rsplit_once(':') {
            Some((h, p)) => (h, p.parse().ok()?),
            None => (rest, if secure { 443 } else { 80 }),
        };
        if host.is_empty() {
            return None;
        }
        Some(Self {
            secure,
            host: host.to_string(),
            port,
        })
    }

    fn ws_url(&self, path: &str) -> String {
        let scheme = if self.secure { "wss" } else { "ws" };
        format!("{scheme}://{}:{}{path}", self.host, self.port)
    }
}

impl fmt::Display for RelayAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scheme = if self.secure { "https" } else { "http" };
        let default = if self.secure { 443 } else { 80 };
        if self.port == default {
            write!(f, "{scheme}://{}", self.host)
        } else {
            write!(f, "{scheme}://{}:{}", self.host, self.port)
        }
    }
}

#[derive(Debug)]
pub enum RelayError {
    Status(u16),
    Other(String),
}

impl fmt::Display for RelayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelayError::Status(404) => write!(f, "not found (user offline or invite expired)"),
            RelayError::Status(409) => write!(f, "name already online on relay"),
            RelayError::Status(code) => write!(f, "relay answered HTTP {code}"),
            RelayError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<io::Error> for RelayError {
    fn from(e: io::Error) -> Self {
        RelayError::Other(e.to_string())
    }
}

fn enc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn tls_config() -> Arc<ClientConfig> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let mut config =
                ClientConfig::builder_with_provider(Arc::new(ring_provider::default_provider()))
                    .with_safe_default_protocol_versions()
                    .expect("ring supports default TLS versions")
                    .with_root_certificates(roots)
                    .with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            Arc::new(config)
        })
        .clone()
}

async fn open_ws(addr: &RelayAddr, path: &str, session: Option<&str>) -> Result<Ws, RelayError> {
    let mut request = addr
        .ws_url(path)
        .into_client_request()
        .map_err(|e| RelayError::Other(e.to_string()))?;
    if let Some(session) = session {
        let value = HeaderValue::from_str(session).map_err(|e| RelayError::Other(e.to_string()))?;
        request.headers_mut().insert(SESSION_HEADER, value);
    }
    let connect =
        connect_async_tls_with_config(request, None, false, Some(Connector::Rustls(tls_config())));
    match timeout(CONNECT_TIMEOUT, connect).await {
        Err(_) => Err(RelayError::Other("relay did not answer in time".into())),
        Ok(Err(WsError::Http(resp))) => Err(RelayError::Status(resp.status().as_u16())),
        Ok(Err(e)) => Err(RelayError::Other(e.to_string())),
        Ok(Ok((ws, _))) => Ok(ws),
    }
}

async fn exchange<S>(mut stream: S, request: &str) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    let mut raw = Vec::new();
    match stream.take(MAX_HTTP_RESPONSE).read_to_end(&mut raw).await {
        Ok(_) => Ok(raw),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && !raw.is_empty() => Ok(raw),
        Err(e) => Err(e),
    }
}

fn dechunk(mut body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(end) = body.windows(2).position(|w| w == b"\r\n") {
        let size_line = String::from_utf8_lossy(&body[..end]);
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let Ok(size) = usize::from_str_radix(size_hex, 16) else {
            break;
        };
        body = &body[end + 2..];
        if size == 0 || body.len() < size {
            break;
        }
        out.extend_from_slice(&body[..size]);
        body = body.get(size + 2..).unwrap_or(&[]);
    }
    out
}

fn parse_http(raw: &[u8]) -> Result<(u16, String), RelayError> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| RelayError::Other("malformed HTTP response".into()))?;
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RelayError::Other("malformed HTTP status".into()))?;
    let body = &raw[split + 4..];
    let body = if head.contains("transfer-encoding: chunked") {
        dechunk(body)
    } else {
        body.to_vec()
    };
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

async fn http_get(
    addr: &RelayAddr,
    path: &str,
    session: &str,
) -> Result<(u16, String), RelayError> {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: cli-net-comm\r\n{SESSION_HEADER}: {session}\r\nConnection: close\r\n\r\n",
        addr.host
    );
    let fetch = async {
        let tcp = TcpStream::connect((addr.host.as_str(), addr.port)).await?;
        if addr.secure {
            let name = ServerName::try_from(addr.host.clone())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let tls = TlsConnector::from(tls_config()).connect(name, tcp).await?;
            exchange(tls, &request).await
        } else {
            exchange(tcp, &request).await
        }
    };
    let raw = timeout(HTTP_TIMEOUT, fetch)
        .await
        .map_err(|_| RelayError::Other("relay did not answer in time".into()))??;
    parse_http(&raw)
}

pub async fn connect(
    addr: &RelayAddr,
    from: &str,
    to: &str,
    session: &str,
) -> Result<RelayStream, RelayError> {
    let path = format!("/connect?from={}&to={}", enc(from), enc(to));
    open_ws(addr, &path, Some(session)).await.map(WsBytes::new)
}

pub async fn join(
    addr: &RelayAddr,
    token: &str,
    name: &str,
    session: &str,
) -> Result<RelayStream, RelayError> {
    let path = format!("/join/{}?name={}", enc(token), enc(name));
    open_ws(addr, &path, Some(session)).await.map(WsBytes::new)
}

pub async fn decline(
    addr: &RelayAddr,
    token: &str,
    name: &str,
    session: &str,
) -> Result<(), RelayError> {
    let path = format!("/decline/{}?name={}", enc(token), enc(name));
    let (status, _) = http_get(addr, &path, session).await?;
    match status {
        200..=299 => Ok(()),
        code => Err(RelayError::Status(code)),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RelayStatus {
    #[default]
    Off,
    NeedsName,
    Connecting,
    Online,
    Error(String),
}

impl fmt::Display for RelayStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelayStatus::Off => write!(f, "off"),
            RelayStatus::NeedsName => write!(f, "waiting for you to set a name"),
            RelayStatus::Connecting => {
                write!(f, "connecting… (free Render may need ~1 min to wake)")
            }
            RelayStatus::Online => write!(f, "online"),
            RelayStatus::Error(e) => write!(f, "error: {e} (retrying)"),
        }
    }
}

#[derive(Default)]
struct StateInner {
    status: RelayStatus,
    users: Vec<String>,
    session: Option<String>,
}

#[derive(Clone, Default)]
pub struct RelayState {
    inner: Arc<Mutex<StateInner>>,
}

impl RelayState {
    pub fn status(&self) -> RelayStatus {
        self.inner.lock().unwrap().status.clone()
    }

    pub fn users(&self) -> Vec<String> {
        self.inner.lock().unwrap().users.clone()
    }

    pub fn session(&self) -> Option<String> {
        self.inner.lock().unwrap().session.clone()
    }

    fn set_session(&self, session: Option<String>) {
        self.inner.lock().unwrap().session = session;
    }

    fn set_status(&self, status: RelayStatus, online: &OnlineList) {
        let mut inner = self.inner.lock().unwrap();
        if inner.status != status {
            inner.status = status;
            drop(inner);
            online.touch();
        }
    }

    fn set_users(&self, mut users: Vec<String>, online: &OnlineList) {
        users.sort();
        let mut inner = self.inner.lock().unwrap();
        if inner.users != users {
            inner.users = users;
            drop(inner);
            online.touch();
        }
    }
}

pub async fn run(
    mut config_rx: watch::Receiver<Config>,
    online: OnlineList,
    invites: Invites,
    state: RelayState,
) {
    let mut backoff = Duration::from_secs(2);
    loop {
        let cfg = config_rx.borrow().clone();
        let Some(addr) = cfg.relay.clone() else {
            state.set_users(Vec::new(), &online);
            state.set_status(RelayStatus::Off, &online);
            if config_rx.changed().await.is_err() {
                return;
            }
            continue;
        };

        if !cfg.has_name() {
            state.set_users(Vec::new(), &online);
            state.set_status(RelayStatus::NeedsName, &online);
            if config_rx.changed().await.is_err() {
                return;
            }
            continue;
        }

        state.set_status(RelayStatus::Connecting, &online);
        let path = format!("/listen?name={}", enc(&cfg.username));
        match open_ws(&addr, &path, None).await {
            Ok(ws) => {
                backoff = Duration::from_secs(2);
                state.set_status(RelayStatus::Online, &online);
                let ended = session(
                    ws,
                    &addr,
                    &cfg.username,
                    &mut config_rx,
                    &online,
                    &invites,
                    &state,
                )
                .await;
                state.set_session(None);
                state.set_users(Vec::new(), &online);
                match ended {
                    SessionEnd::ConfigChanged => continue,
                    SessionEnd::Shutdown => return,
                    SessionEnd::Lost(e) => state.set_status(RelayStatus::Error(e), &online),
                }
            }
            Err(e) => state.set_status(RelayStatus::Error(e.to_string()), &online),
        }

        tokio::select! {
            _ = sleep(backoff) => {}
            changed = config_rx.changed() => if changed.is_err() { return },
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

enum SessionEnd {
    ConfigChanged,
    Shutdown,
    Lost(String),
}

async fn session(
    ws: Ws,
    addr: &RelayAddr,
    me: &str,
    config_rx: &mut watch::Receiver<Config>,
    online: &OnlineList,
    invites: &Invites,
    state: &RelayState,
) -> SessionEnd {
    let (mut sink, mut stream) = ws.split();
    let mut keepalive = interval_at(Instant::now() + KEEPALIVE, KEEPALIVE);
    let mut pending = String::new();

    loop {
        tokio::select! {
            msg = stream.next() => {
                let data = match msg {
                    Some(Ok(Message::Binary(data))) => data,
                    Some(Ok(Message::Text(text))) => Bytes::from(text),
                    Some(Ok(Message::Close(_))) | None => {
                        return SessionEnd::Lost("relay closed the connection".into());
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => return SessionEnd::Lost(e.to_string()),
                };
                pending.push_str(&String::from_utf8_lossy(&data));
                if pending.len() > MAX_HTTP_RESPONSE as usize {
                    return SessionEnd::Lost("relay sent an oversized message".into());
                }
                while let Some(end) = pending.find('\n') {
                    let line: String = pending.drain(..=end).collect();
                    handle_line(line.trim(), addr, me, online, invites, state);
                }
            }
            _ = keepalive.tick() => {
                if let Err(e) = sink.send(Message::Ping(Bytes::new())).await {
                    return SessionEnd::Lost(e.to_string());
                }
            }
            changed = config_rx.changed() => {
                if changed.is_err() {
                    return SessionEnd::Shutdown;
                }
                let cfg = config_rx.borrow().clone();
                if cfg.relay.as_ref() != Some(addr) || cfg.username != me {
                    let _ = sink.send(Message::Close(None)).await;
                    return SessionEnd::ConfigChanged;
                }
            }
        }
    }
}

fn handle_line(
    line: &str,
    addr: &RelayAddr,
    me: &str,
    online: &OnlineList,
    invites: &Invites,
    state: &RelayState,
) {
    let parts = extract_brackets(line);
    if line.starts_with("SESSION") {
        state.set_session(parts.first().map(|s| s.to_string()));
        return;
    }
    if line.starts_with("USERS") {
        let others = parts
            .into_iter()
            .filter(|name| *name != me)
            .map(String::from)
            .collect();
        state.set_users(others, online);
        return;
    }
    if !line.starts_with("INVITE") {
        return;
    }
    let (Some(from), Some(token), Some(session)) = (parts.first(), parts.get(1), state.session())
    else {
        return;
    };
    let source = InviteSource::Relay {
        addr: addr.clone(),
        token: token.to_string(),
        session: session.clone(),
    };
    let origin = format!("relay {}", addr.host);
    let accepted = !is_in_chat() && invites.offer(from.to_string(), origin, source).is_ok();
    if accepted {
        online.touch();
    } else {
        let (addr, me, token) = (addr.clone(), me.to_string(), token.to_string());
        tokio::spawn(async move {
            let _ = decline(&addr, &token, &me, &session).await;
        });
    }
}
