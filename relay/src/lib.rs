use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::{
        ConnectInfo, Path, Query, Request, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    time::{Instant, Interval, interval_at, timeout},
};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const FERRIS_PNG: &[u8] = include_bytes!("../assets/ferris.png");
const LOGO_SVG: &str = include_str!("../assets/logo-dark.svg");

pub const SESSION_HEADER: &str = "x-session";

const INVITE_TIMEOUT: Duration = Duration::from_secs(60);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const MAX_NAME_CHARS: usize = 32;
const MAX_WS_MESSAGE: usize = 64 * 1024;
const MAX_LISTENERS: usize = 1000;
const MAX_LISTENERS_PER_IP: usize = 5;
const MAX_PENDING: usize = 1000;
const MAX_PENDING_PER_TARGET: usize = 5;
const MAX_PENDING_PER_INVITER: usize = 3;
const MAX_ACTIVE_CHATS: usize = 200;
const LISTENER_QUEUE: usize = 32;
const CHAT_IDLE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const CHAT_MAX_DURATION: Duration = Duration::from_secs(12 * 60 * 60);
const CHAT_MAX_BYTES: usize = 64 * 1024 * 1024;
const RATE_WINDOW: Duration = Duration::from_secs(60);
const RATE_LISTEN: u32 = 10;
const RATE_CONNECT: u32 = 20;
const RATE_OTHER: u32 = 120;

const SECURITY_HEADERS: [(&str, &str); 6] = [
    (
        "content-security-policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
    ),
    ("strict-transport-security", "max-age=31536000"),
    ("x-content-type-options", "nosniff"),
    ("x-frame-options", "DENY"),
    ("referrer-policy", "no-referrer"),
    (
        "permissions-policy",
        "camera=(), microphone=(), geolocation=()",
    ),
];

type Params = Query<HashMap<String, String>>;
type RateTable = HashMap<(IpAddr, &'static str), (Instant, u32)>;

struct Listener {
    session: String,
    ip: IpAddr,
    tx: mpsc::Sender<String>,
}

struct Pending {
    from: String,
    to: String,
    inviter: WebSocket,
}

struct Relay {
    listeners: Mutex<HashMap<String, Listener>>,
    pending: Mutex<HashMap<String, Pending>>,
    rate: Mutex<RateTable>,
    users: watch::Sender<Arc<Vec<String>>>,
    active_chats: AtomicUsize,
    started: Instant,
    trust_proxy: bool,
    public_users: bool,
}

#[derive(Clone, Copy, Default)]
pub struct Settings {
    pub trust_proxy: bool,
    pub public_users: bool,
}

impl Settings {
    pub fn from_env() -> Self {
        let on = |key: &str| {
            std::env::var(key).is_ok_and(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        };
        Self {
            trust_proxy: on("TRUST_PROXY"),
            public_users: on("PUBLIC_USERS"),
        }
    }
}

type Shared = Arc<Relay>;

struct ChatGuard<'a>(&'a AtomicUsize);

impl Drop for ChatGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn app(settings: Settings) -> Router {
    let (users, _) = watch::channel(Arc::new(Vec::new()));
    let relay = Arc::new(Relay {
        listeners: Mutex::default(),
        pending: Mutex::default(),
        rate: Mutex::default(),
        users,
        active_chats: AtomicUsize::default(),
        started: Instant::now(),
        trust_proxy: settings.trust_proxy,
        public_users: settings.public_users,
    });
    Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/ferris.png", get(ferris))
        .route("/logo.svg", get(logo))
        .route("/status", get(status))
        .route("/healthz", get(|| async { "ok" }))
        .route("/listen", get(listen))
        .route("/connect", get(connect))
        .route("/join/{token}", get(join))
        .route("/decline/{token}", get(decline))
        .layer(middleware::from_fn(security_headers))
        .with_state(relay)
}

pub async fn serve(listener: TcpListener, settings: Settings) -> std::io::Result<()> {
    axum::serve(
        listener,
        app(settings).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}

async fn security_headers(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    let headers = res.headers_mut();
    for (name, value) in SECURITY_HEADERS {
        headers.insert(name, HeaderValue::from_static(value));
    }
    res
}

fn random_token() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("OS random number generator");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn valid_name(name: &str) -> bool {
    let chars = name.chars().count();
    chars > 0
        && chars <= MAX_NAME_CHARS
        && name.trim() == name
        && !name.chars().any(|c| c.is_control() || c == '<' || c == '>')
}

fn param<'a>(q: &'a Params, key: &str) -> Option<&'a str> {
    q.get(key).map(String::as_str).filter(|n| valid_name(n))
}

fn session_of(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(SESSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.len() == 32)
}

fn client_ip(relay: &Relay, peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
    if relay.trust_proxy
        && let Some(ip) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .and_then(|v| v.trim().parse().ok())
    {
        return ip;
    }
    peer.ip()
}

fn rate_ok(relay: &Relay, ip: IpAddr, bucket: &'static str, limit: u32) -> bool {
    let mut rate = relay.rate.lock().unwrap();
    if rate.len() > 10_000 {
        rate.retain(|_, (start, _)| start.elapsed() < RATE_WINDOW);
    }
    let entry = rate.entry((ip, bucket)).or_insert((Instant::now(), 0));
    if entry.0.elapsed() >= RATE_WINDOW {
        *entry = (Instant::now(), 0);
    }
    entry.1 += 1;
    entry.1 <= limit
}

fn reject(code: StatusCode, msg: &'static str) -> Response {
    (code, msg).into_response()
}

fn pinger() -> Interval {
    interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL)
}

fn line(text: String) -> Message {
    Message::Binary(Bytes::from(text))
}

fn publish_users(relay: &Relay) {
    let mut names: Vec<String> = relay.listeners.lock().unwrap().keys().cloned().collect();
    names.sort();
    relay.users.send_replace(Arc::new(names));
}

fn users_line(users: &[String]) -> String {
    let mut out = String::from("USERS");
    for name in users {
        out.push_str(" <");
        out.push_str(name);
        out.push('>');
    }
    out.push('\n');
    out
}

fn limit_ws(ws: WebSocketUpgrade) -> WebSocketUpgrade {
    ws.max_message_size(MAX_WS_MESSAGE)
        .max_frame_size(MAX_WS_MESSAGE)
}

async fn ferris() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        FERRIS_PNG,
    )
}

async fn logo() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        LOGO_SVG,
    )
}

fn json_string(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

async fn status(
    State(relay): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if !rate_ok(
        &relay,
        client_ip(&relay, peer, &headers),
        "other",
        RATE_OTHER,
    ) {
        return reject(StatusCode::TOO_MANY_REQUESTS, "slow down");
    }
    let users = relay.users.borrow().clone();
    let names = if relay.public_users {
        let list: Vec<String> = users.iter().map(|n| json_string(n)).collect();
        format!(",\"users\":[{}]", list.join(","))
    } else {
        String::new()
    };
    let body = format!(
        "{{\"online\":{},\"active_chats\":{},\"pending_invites\":{},\"uptime_secs\":{}{}}}",
        users.len(),
        relay.active_chats.load(Ordering::Relaxed),
        relay.pending.lock().unwrap().len(),
        relay.started.elapsed().as_secs(),
        names
    );
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

async fn listen(
    State(relay): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    q: Params,
    ws: WebSocketUpgrade,
) -> Response {
    let ip = client_ip(&relay, peer, &headers);
    if !rate_ok(&relay, ip, "listen", RATE_LISTEN) {
        return reject(StatusCode::TOO_MANY_REQUESTS, "slow down");
    }
    let Some(name) = param(&q, "name").map(str::to_string) else {
        return reject(StatusCode::BAD_REQUEST, "missing or invalid name");
    };

    let session = random_token();
    let (tx, mut rx) = mpsc::channel::<String>(LISTENER_QUEUE);
    {
        let mut listeners = relay.listeners.lock().unwrap();
        listeners.retain(|_, l| !l.tx.is_closed());
        if listeners.contains_key(&name) {
            return reject(StatusCode::CONFLICT, "name already online");
        }
        if listeners.len() >= MAX_LISTENERS {
            return reject(StatusCode::SERVICE_UNAVAILABLE, "relay is full");
        }
        if listeners.values().filter(|l| l.ip == ip).count() >= MAX_LISTENERS_PER_IP {
            return reject(
                StatusCode::TOO_MANY_REQUESTS,
                "too many users from your address",
            );
        }
        listeners.insert(
            name.clone(),
            Listener {
                session: session.clone(),
                ip,
                tx: tx.clone(),
            },
        );
    }
    publish_users(&relay);

    limit_ws(ws).on_upgrade(move |socket| async move {
        let (mut sink, mut stream) = socket.split();
        let mut users = relay.users.subscribe();
        users.mark_changed();
        let mut ping = pinger();
        let mut ok = sink
            .send(line(format!("SESSION <{session}>\n")))
            .await
            .is_ok();
        while ok {
            tokio::select! {
                out = rx.recv() => {
                    let Some(text) = out else { break };
                    ok = sink.send(line(text)).await.is_ok();
                }
                changed = users.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let list = users.borrow_and_update().clone();
                    ok = sink.send(line(users_line(&list))).await.is_ok();
                }
                _ = ping.tick() => {
                    ok = sink.send(Message::Ping(Bytes::new())).await.is_ok();
                }
                incoming = stream.next() => match incoming {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                },
            }
        }
        {
            let mut listeners = relay.listeners.lock().unwrap();
            if listeners.get(&name).is_some_and(|l| l.session == session) {
                listeners.remove(&name);
            }
        }
        relay
            .pending
            .lock()
            .unwrap()
            .retain(|_, p| p.to != name && p.from != name);
        publish_users(&relay);
    })
}

async fn connect(
    State(relay): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    q: Params,
    ws: WebSocketUpgrade,
) -> Response {
    let ip = client_ip(&relay, peer, &headers);
    if !rate_ok(&relay, ip, "connect", RATE_CONNECT) {
        return reject(StatusCode::TOO_MANY_REQUESTS, "slow down");
    }
    let (Some(from), Some(to)) = (param(&q, "from"), param(&q, "to")) else {
        return reject(StatusCode::BAD_REQUEST, "missing or invalid from/to");
    };
    let (from, to) = (from.to_string(), to.to_string());
    let Some(session) = session_of(&headers) else {
        return reject(StatusCode::UNAUTHORIZED, "missing session");
    };

    let target = {
        let listeners = relay.listeners.lock().unwrap();
        if listeners.get(&from).is_none_or(|l| l.session != session) {
            return reject(
                StatusCode::UNAUTHORIZED,
                "you are not online under this name",
            );
        }
        match listeners.get(&to) {
            Some(l) => l.tx.clone(),
            None => return reject(StatusCode::NOT_FOUND, "user offline"),
        }
    };
    {
        let pending = relay.pending.lock().unwrap();
        if pending.len() >= MAX_PENDING
            || pending.values().filter(|p| p.to == to).count() >= MAX_PENDING_PER_TARGET
            || pending.values().filter(|p| p.from == from).count() >= MAX_PENDING_PER_INVITER
        {
            return reject(StatusCode::TOO_MANY_REQUESTS, "too many pending invites");
        }
    }

    limit_ws(ws).on_upgrade(move |inviter| async move {
        let token = random_token();
        relay.pending.lock().unwrap().insert(
            token.clone(),
            Pending {
                from: from.clone(),
                to,
                inviter,
            },
        );
        if target
            .try_send(format!("INVITE <{from}> <{token}>\n"))
            .is_err()
        {
            relay.pending.lock().unwrap().remove(&token);
            return;
        }
        tokio::time::sleep(INVITE_TIMEOUT).await;
        relay.pending.lock().unwrap().remove(&token);
    })
}

fn take_pending(relay: &Relay, token: &str, name: &str, session: &str) -> Option<Pending> {
    let authorized = relay
        .listeners
        .lock()
        .unwrap()
        .get(name)
        .is_some_and(|l| l.session == session);
    if !authorized {
        return None;
    }
    let mut pending = relay.pending.lock().unwrap();
    match pending.get(token) {
        Some(p) if p.to == name => pending.remove(token),
        _ => None,
    }
}

async fn join(
    State(relay): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(token): Path<String>,
    q: Params,
    ws: WebSocketUpgrade,
) -> Response {
    if !rate_ok(
        &relay,
        client_ip(&relay, peer, &headers),
        "other",
        RATE_OTHER,
    ) {
        return reject(StatusCode::TOO_MANY_REQUESTS, "slow down");
    }
    let Some(name) = param(&q, "name").map(str::to_string) else {
        return reject(StatusCode::BAD_REQUEST, "missing or invalid name");
    };
    let Some(session) = session_of(&headers) else {
        return reject(StatusCode::UNAUTHORIZED, "missing session");
    };
    if relay.active_chats.load(Ordering::Relaxed) >= MAX_ACTIVE_CHATS {
        return reject(StatusCode::SERVICE_UNAVAILABLE, "relay is full");
    }
    let Some(Pending { mut inviter, .. }) = take_pending(&relay, &token, &name, session) else {
        return reject(StatusCode::NOT_FOUND, "invite expired");
    };

    relay.active_chats.fetch_add(1, Ordering::Relaxed);
    limit_ws(ws).on_upgrade(move |invitee| async move {
        let _chat = ChatGuard(&relay.active_chats);
        if inviter
            .send(line(format!("ACCEPT <{name}>\n")))
            .await
            .is_err()
        {
            return;
        }
        let _ = timeout(CHAT_MAX_DURATION, pipe(inviter, invitee)).await;
    })
}

async fn pipe(a: WebSocket, b: WebSocket) {
    let (mut a_tx, mut a_rx) = a.split();
    let (mut b_tx, mut b_rx) = b.split();
    let mut ping = pinger();
    let mut last_data = Instant::now();
    let mut total = 0usize;
    loop {
        let idle_left = CHAT_IDLE_TIMEOUT.saturating_sub(last_data.elapsed());
        let (msg, target) = tokio::select! {
            m = a_rx.next() => (m, &mut b_tx),
            m = b_rx.next() => (m, &mut a_tx),
            _ = tokio::time::sleep(idle_left) => break,
            _ = ping.tick() => {
                let ping_a = a_tx.send(Message::Ping(Bytes::new())).await;
                let ping_b = b_tx.send(Message::Ping(Bytes::new())).await;
                if ping_a.is_err() || ping_b.is_err() {
                    break;
                }
                continue;
            }
        };
        let data = match msg {
            Some(Ok(Message::Binary(data))) => data,
            Some(Ok(Message::Text(text))) => Bytes::from(text),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            _ => break,
        };
        total += data.len();
        if total > CHAT_MAX_BYTES {
            break;
        }
        last_data = Instant::now();
        if target.send(Message::Binary(data)).await.is_err() {
            break;
        }
    }
    let _ = a_tx.send(Message::Close(None)).await;
    let _ = b_tx.send(Message::Close(None)).await;
}

async fn decline(
    State(relay): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(token): Path<String>,
    q: Params,
) -> StatusCode {
    if !rate_ok(
        &relay,
        client_ip(&relay, peer, &headers),
        "other",
        RATE_OTHER,
    ) {
        return StatusCode::TOO_MANY_REQUESTS;
    }
    let Some(name) = param(&q, "name") else {
        return StatusCode::BAD_REQUEST;
    };
    let Some(session) = session_of(&headers) else {
        return StatusCode::UNAUTHORIZED;
    };
    let Some(Pending { mut inviter, .. }) = take_pending(&relay, &token, name, session) else {
        return StatusCode::NOT_FOUND;
    };
    tokio::spawn(async move {
        let _ = inviter.send(line("DECLINE\n".to_string())).await;
        let _ = inviter.send(Message::Close(None)).await;
    });
    StatusCode::NO_CONTENT
}
