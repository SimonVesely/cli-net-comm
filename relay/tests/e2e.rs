use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, connect_async,
    tungstenite::{Error, client::IntoClientRequest, http::HeaderValue},
};
use utils::ws::WsBytes;

type Ws = WsBytes<MaybeTlsStream<TcpStream>>;

async fn start_with(settings: relay::Settings) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { relay::serve(listener, settings).await.unwrap() });
    addr
}

async fn start() -> SocketAddr {
    start_with(relay::Settings::default()).await
}

async fn open(addr: SocketAddr, path: &str, session: Option<&str>) -> Result<Ws, u16> {
    let mut req = format!("ws://{addr}{path}").into_client_request().unwrap();
    if let Some(s) = session {
        req.headers_mut()
            .insert(relay::SESSION_HEADER, HeaderValue::from_str(s).unwrap());
    }
    match connect_async(req).await {
        Ok((ws, _)) => Ok(WsBytes::new(ws)),
        Err(Error::Http(resp)) => Err(resp.status().as_u16()),
        Err(e) => panic!("{e}"),
    }
}

async fn read_line(s: &mut Ws) -> String {
    let mut buf = Vec::new();
    let mut b = [0u8; 1];
    while s.read(&mut b).await.unwrap() == 1 && b[0] != b'\n' {
        buf.push(b[0]);
    }
    String::from_utf8(buf).unwrap()
}

async fn next_with(s: &mut Ws, prefix: &str) -> String {
    timeout(Duration::from_secs(5), async {
        loop {
            let line = read_line(s).await;
            if line.starts_with(prefix) {
                return line;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {prefix} line"))
}

fn bracket(line: &str, index: usize) -> String {
    line.split('<')
        .skip(1)
        .nth(index)
        .unwrap()
        .split('>')
        .next()
        .unwrap()
        .to_string()
}

struct User {
    ws: Ws,
    session: String,
}

async fn login(addr: SocketAddr, name: &str) -> User {
    let mut ws = open(addr, &format!("/listen?name={name}"), None)
        .await
        .unwrap();
    let session = bracket(&next_with(&mut ws, "SESSION").await, 0);
    User { ws, session }
}

async fn get(addr: SocketAddr, path: &str, session: Option<&str>) -> String {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let extra = session.map_or(String::new(), |s| {
        format!("{}: {s}\r\n", relay::SESSION_HEADER)
    });
    s.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: relay\r\n{extra}Connection: close\r\n\r\n")
            .as_bytes(),
    )
    .await
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

fn status_code(response: &str) -> u16 {
    response.split_whitespace().nth(1).unwrap().parse().unwrap()
}

#[tokio::test]
async fn invite_accept_tls_through_relay() {
    let addr = start().await;
    let mut bob = login(addr, "Bob").await;
    let alice = login(addr, "Alice").await;
    timeout(Duration::from_secs(5), async {
        while !next_with(&mut bob.ws, "USERS").await.contains("<Alice>") {}
    })
    .await
    .expect("Bob is told that Alice came online");

    let mut inviter = open(addr, "/connect?from=Alice&to=Bob", Some(&alice.session))
        .await
        .unwrap();
    let invite = next_with(&mut bob.ws, "INVITE").await;
    assert_eq!(bracket(&invite, 0), "Alice");
    let token = bracket(&invite, 1);
    assert_eq!(token.len(), 32);

    let joined = open(addr, &format!("/join/{token}?name=Bob"), Some(&bob.session))
        .await
        .unwrap();
    assert_eq!(read_line(&mut inviter).await, "ACCEPT <Bob>");

    let bob_side = tokio::spawn(async move {
        let (tls, fp) = utils::tls::accept(joined).await.unwrap();
        let (rd, mut wr) = tokio::io::split(tls);
        wr.write_all(b"MSG ahoj\n").await.unwrap();
        wr.flush().await.unwrap();
        let mut lines = BufReader::new(rd).lines();
        assert_eq!(lines.next_line().await.unwrap().unwrap(), "MSG cau");
        fp
    });
    let (tls, fp_alice) = utils::tls::connect(inviter).await.unwrap();
    let (rd, mut wr) = tokio::io::split(tls);
    let mut lines = BufReader::new(rd).lines();
    assert_eq!(lines.next_line().await.unwrap().unwrap(), "MSG ahoj");
    wr.write_all(b"MSG cau\n").await.unwrap();
    wr.flush().await.unwrap();
    assert_eq!(fp_alice, bob_side.await.unwrap());
}

#[tokio::test]
async fn invite_cannot_be_hijacked() {
    let addr = start().await;
    let mut bob = login(addr, "Bob").await;
    let alice = login(addr, "Alice").await;
    let mallory = login(addr, "Mallory").await;

    let _inviter = open(addr, "/connect?from=Alice&to=Bob", Some(&alice.session))
        .await
        .unwrap();
    let token = bracket(&next_with(&mut bob.ws, "INVITE").await, 1);

    assert_eq!(
        open(addr, "/join/0?name=Bob", Some(&mallory.session))
            .await
            .err(),
        Some(404)
    );
    assert_eq!(
        open(
            addr,
            &format!("/join/{token}?name=Bob"),
            Some(&mallory.session)
        )
        .await
        .err(),
        Some(404),
        "knowing the token is not enough without Bob's session"
    );
    assert_eq!(
        open(
            addr,
            &format!("/join/{token}?name=Mallory"),
            Some(&mallory.session)
        )
        .await
        .err(),
        Some(404)
    );
    assert_eq!(
        open(addr, &format!("/join/{token}?name=Bob"), None)
            .await
            .err(),
        Some(401)
    );
    assert!(
        open(addr, &format!("/join/{token}?name=Bob"), Some(&bob.session))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn sender_name_cannot_be_spoofed() {
    let addr = start().await;
    let _bob = login(addr, "Bob").await;
    let _alice = login(addr, "Alice").await;
    let mallory = login(addr, "Mallory").await;

    assert_eq!(
        open(addr, "/connect?from=Alice&to=Bob", Some(&mallory.session))
            .await
            .err(),
        Some(401)
    );
    assert_eq!(
        open(addr, "/connect?from=Alice&to=Bob", None).await.err(),
        Some(401)
    );
    assert_eq!(
        open(addr, "/connect?from=Ghost&to=Bob", Some(&mallory.session))
            .await
            .err(),
        Some(401)
    );
}

#[tokio::test]
async fn only_invitee_can_decline() {
    let addr = start().await;
    let mut bob = login(addr, "Bob").await;
    let alice = login(addr, "Alice").await;
    let mallory = login(addr, "Mallory").await;

    let mut inviter = open(addr, "/connect?from=Alice&to=Bob", Some(&alice.session))
        .await
        .unwrap();
    let token = bracket(&next_with(&mut bob.ws, "INVITE").await, 1);

    let path = format!("/decline/{token}?name=Bob");
    assert_eq!(
        status_code(&get(addr, &path, Some(&mallory.session)).await),
        404
    );
    assert_eq!(status_code(&get(addr, &path, None).await), 401);
    assert_eq!(
        status_code(&get(addr, &path, Some(&bob.session)).await),
        204
    );
    assert_eq!(read_line(&mut inviter).await, "DECLINE");
}

#[tokio::test]
async fn names_are_validated() {
    let addr = start().await;
    assert_eq!(
        open(addr, "/listen?name=%1B%5D0%3Bpwned%07Eve", None)
            .await
            .err(),
        Some(400)
    );
    assert_eq!(
        open(addr, "/listen?name=%20Bob", None).await.err(),
        Some(400)
    );
    assert_eq!(
        open(addr, "/listen?name=B%3Cob", None).await.err(),
        Some(400)
    );
    let czech = "%C5%A0imon%20Vesel%C3%BD%20%C5%BElu%C5%A5ou%C4%8Dk%C3%BD%20k%C5%AF%C5%88";
    assert!(
        open(addr, &format!("/listen?name={czech}"), None)
            .await
            .is_ok()
    );
    let _taken = login(addr, "Simon").await;
    assert_eq!(
        open(addr, "/listen?name=Simon", None).await.err(),
        Some(409)
    );
}

#[tokio::test]
async fn invite_floods_are_limited() {
    let addr = start().await;
    let mut bob = login(addr, "Bob").await;
    let alice = login(addr, "Alice").await;

    let mut held = Vec::new();
    for _ in 0..3 {
        held.push(
            open(addr, "/connect?from=Alice&to=Bob", Some(&alice.session))
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        open(addr, "/connect?from=Alice&to=Bob", Some(&alice.session))
            .await
            .err(),
        Some(429),
        "one inviter can have at most 3 pending invites"
    );
    for _ in 0..3 {
        next_with(&mut bob.ws, "INVITE").await;
    }
}

#[tokio::test]
async fn listeners_per_ip_are_limited() {
    let addr = start().await;
    let mut held = Vec::new();
    for i in 0..5 {
        held.push(login(addr, &format!("user{i}")).await);
    }
    assert_eq!(
        open(addr, "/listen?name=user5", None).await.err(),
        Some(429)
    );
}

#[tokio::test]
async fn status_hides_names_and_sends_security_headers() {
    let addr = start().await;
    let _bob = login(addr, "Bob").await;
    let status = get(addr, "/status", None).await;
    assert!(status.contains("\"online\":1"));
    assert!(!status.contains("Bob"));
    assert_eq!(status_code(&get(addr, "/users", None).await), 404);

    let page = get(addr, "/", None).await.to_ascii_lowercase();
    for header in [
        "content-security-policy:",
        "strict-transport-security:",
        "x-content-type-options: nosniff",
        "x-frame-options: deny",
        "referrer-policy: no-referrer",
    ] {
        assert!(page.contains(header), "missing {header}");
    }

    let public = start_with(relay::Settings {
        trust_proxy: false,
        public_users: true,
    })
    .await;
    let _carol = login(public, "Carol").await;
    assert!(
        get(public, "/status", None)
            .await
            .contains("\"users\":[\"Carol\"]")
    );
}

#[tokio::test]
async fn name_freed_after_disconnect() {
    let addr = start().await;
    let mut bob = login(addr, "Bob").await;
    bob.ws.shutdown().await.unwrap();
    drop(bob);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(get(addr, "/status", None).await.contains("\"online\":0"));
    assert!(open(addr, "/listen?name=Bob", None).await.is_ok());
}
