use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::mpsc,
    time::{Instant, Interval, interval_at},
};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const FERRIS_PNG: &[u8] = include_bytes!("../assets/ferris.png");
const LOGO_SVG: &str = include_str!("../assets/logo-dark.svg");

const INVITE_TIMEOUT: Duration = Duration::from_secs(60);
const PING_INTERVAL: Duration = Duration::from_secs(30);

type Params = Query<HashMap<String, String>>;

struct Pending {
    to: String,
    inviter: WebSocket,
}

struct Relay {
    listeners: Mutex<HashMap<String, mpsc::UnboundedSender<String>>>,
    pending: Mutex<HashMap<u64, Pending>>,
    next_id: AtomicU64,
    active_chats: AtomicUsize,
    started: Instant,
}

impl Default for Relay {
    fn default() -> Self {
        Self {
            listeners: Mutex::default(),
            pending: Mutex::default(),
            next_id: AtomicU64::default(),
            active_chats: AtomicUsize::default(),
            started: Instant::now(),
        }
    }
}

struct ChatGuard<'a>(&'a AtomicUsize);

impl<'a> ChatGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for ChatGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

type Shared = Arc<Relay>;

pub fn app() -> Router {
    Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/ferris.png", get(ferris))
        .route("/logo.svg", get(logo))
        .route("/status", get(status))
        .route("/healthz", get(|| async { "ok" }))
        .route("/users", get(users))
        .route("/listen", get(listen))
        .route("/connect", get(connect))
        .route("/join/{id}", get(join))
        .route("/decline/{id}", get(decline))
        .with_state(Arc::new(Relay::default()))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 32 && !name.contains(['<', '>', '\n', '\r'])
}

fn param<'a>(q: &'a Params, key: &str) -> Option<&'a str> {
    q.get(key).map(String::as_str).filter(|n| valid_name(n))
}

fn pinger() -> Interval {
    interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL)
}

fn line(text: String) -> Message {
    Message::Binary(Bytes::from(text))
}

fn online_names(relay: &Relay) -> Vec<String> {
    let mut names: Vec<String> = relay.listeners.lock().unwrap().keys().cloned().collect();
    names.sort();
    names
}

async fn users(State(relay): State<Shared>) -> String {
    online_names(&relay).into_iter().map(|n| n + "\n").collect()
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
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

async fn status(State(relay): State<Shared>) -> impl IntoResponse {
    let users: Vec<String> = online_names(&relay)
        .iter()
        .map(|n| json_string(n))
        .collect();
    let body = format!(
        "{{\"users\":[{}],\"active_chats\":{},\"pending_invites\":{},\"uptime_secs\":{}}}",
        users.join(","),
        relay.active_chats.load(Ordering::Relaxed),
        relay.pending.lock().unwrap().len(),
        relay.started.elapsed().as_secs()
    );
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
}

async fn listen(State(relay): State<Shared>, q: Params, ws: WebSocketUpgrade) -> Response {
    let Some(name) = param(&q, "name").map(str::to_string) else {
        return (StatusCode::BAD_REQUEST, "missing or invalid name").into_response();
    };

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    {
        let mut listeners = relay.listeners.lock().unwrap();
        if listeners.get(&name).is_some_and(|old| !old.is_closed()) {
            return (StatusCode::CONFLICT, "name already online").into_response();
        }
        listeners.insert(name.clone(), tx.clone());
    }

    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut stream) = socket.split();
        let mut ping = pinger();
        loop {
            tokio::select! {
                out = rx.recv() => {
                    let Some(text) = out else { break };
                    if sink.send(line(text)).await.is_err() {
                        break;
                    }
                }
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                        break;
                    }
                }
                incoming = stream.next() => match incoming {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                },
            }
        }
        let mut listeners = relay.listeners.lock().unwrap();
        if listeners
            .get(&name)
            .is_some_and(|cur| cur.same_channel(&tx))
        {
            listeners.remove(&name);
        }
    })
}

async fn connect(State(relay): State<Shared>, q: Params, ws: WebSocketUpgrade) -> Response {
    let (Some(from), Some(to)) = (param(&q, "from"), param(&q, "to")) else {
        return (StatusCode::BAD_REQUEST, "missing or invalid from/to").into_response();
    };
    let (from, to) = (from.to_string(), to.to_string());
    let Some(listener) = relay.listeners.lock().unwrap().get(&to).cloned() else {
        return (StatusCode::NOT_FOUND, "user offline").into_response();
    };

    ws.on_upgrade(move |inviter| async move {
        let id = relay.next_id.fetch_add(1, Ordering::Relaxed);
        relay.pending.lock().unwrap().insert(
            id,
            Pending {
                to: to.clone(),
                inviter,
            },
        );
        if listener.send(format!("INVITE <{from}> <{id}>\n")).is_err() {
            relay.pending.lock().unwrap().remove(&id);
            return;
        }
        tokio::time::sleep(INVITE_TIMEOUT).await;
        relay.pending.lock().unwrap().remove(&id);
    })
}

fn take_pending(relay: &Relay, id: u64, name: &str) -> Option<Pending> {
    let mut pending = relay.pending.lock().unwrap();
    match pending.get(&id) {
        Some(p) if p.to == name => pending.remove(&id),
        _ => None,
    }
}

async fn join(
    State(relay): State<Shared>,
    Path(id): Path<u64>,
    q: Params,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(name) = param(&q, "name").map(str::to_string) else {
        return (StatusCode::BAD_REQUEST, "missing or invalid name").into_response();
    };
    let Some(Pending { mut inviter, .. }) = take_pending(&relay, id, &name) else {
        return (StatusCode::NOT_FOUND, "invite expired").into_response();
    };

    ws.on_upgrade(move |invitee| async move {
        if inviter
            .send(line(format!("ACCEPT <{name}>\n")))
            .await
            .is_err()
        {
            return;
        }
        let _chat = ChatGuard::new(&relay.active_chats);
        pipe(inviter, invitee).await;
    })
}

async fn pipe(a: WebSocket, b: WebSocket) {
    let (mut a_tx, mut a_rx) = a.split();
    let (mut b_tx, mut b_rx) = b.split();
    let mut ping = pinger();
    loop {
        let (msg, target) = tokio::select! {
            m = a_rx.next() => (m, &mut b_tx),
            m = b_rx.next() => (m, &mut a_tx),
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
        if target.send(Message::Binary(data)).await.is_err() {
            break;
        }
    }
    let _ = a_tx.send(Message::Close(None)).await;
    let _ = b_tx.send(Message::Close(None)).await;
}

async fn decline(State(relay): State<Shared>, Path(id): Path<u64>, q: Params) -> StatusCode {
    let Some(name) = param(&q, "name") else {
        return StatusCode::BAD_REQUEST;
    };
    let Some(Pending { mut inviter, .. }) = take_pending(&relay, id, name) else {
        return StatusCode::NOT_FOUND;
    };
    tokio::spawn(async move {
        let _ = inviter.send(line("DECLINE\n".to_string())).await;
        let _ = inviter.send(Message::Close(None)).await;
    });
    StatusCode::NO_CONTENT
}
