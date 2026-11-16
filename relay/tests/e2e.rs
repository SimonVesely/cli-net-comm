use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tokio_tungstenite::{MaybeTlsStream, connect_async, tungstenite::Error};
use utils::ws::WsBytes;

type Ws = WsBytes<MaybeTlsStream<TcpStream>>;

async fn start() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, relay::app()).await.unwrap() });
    addr
}

async fn open(addr: SocketAddr, path: &str) -> Result<Ws, u16> {
    match connect_async(format!("ws://{addr}{path}")).await {
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

fn invite_id(invite: &str) -> String {
    invite
        .rsplit('<')
        .next()
        .unwrap()
        .trim_end_matches('>')
        .to_string()
}

async fn get(addr: SocketAddr, path: &str) -> String {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: relay\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn invite_accept_tls_through_relay() {
    let addr = start().await;
    assert!(get(addr, "/healthz").await.ends_with("ok"));

    let mut bob_listen = open(addr, "/listen?name=Bob").await.unwrap();
    assert!(get(addr, "/users").await.ends_with("Bob\n"));
    assert_eq!(open(addr, "/listen?name=Bob").await.err(), Some(409));
    assert_eq!(
        open(addr, "/connect?from=Alice&to=Nobody").await.err(),
        Some(404)
    );

    let mut alice = open(addr, "/connect?from=Alice&to=Bob").await.unwrap();
    let invite = read_line(&mut bob_listen).await;
    assert!(invite.starts_with("INVITE <Alice> <"), "{invite}");
    let id = invite_id(&invite);

    let bob = open(addr, &format!("/join/{id}?name=Bob")).await.unwrap();
    assert_eq!(read_line(&mut alice).await, "ACCEPT <Bob>");

    let bob_side = tokio::spawn(async move {
        let (tls, fp) = utils::tls::accept(bob).await.unwrap();
        let (rd, mut wr) = tokio::io::split(tls);
        wr.write_all(b"MSG ahoj\n").await.unwrap();
        wr.flush().await.unwrap();
        let mut lines = BufReader::new(rd).lines();
        assert_eq!(lines.next_line().await.unwrap().unwrap(), "MSG cau");
        fp
    });

    let (tls, fp_alice) = utils::tls::connect(alice).await.unwrap();
    let (rd, mut wr) = tokio::io::split(tls);
    let mut lines = BufReader::new(rd).lines();
    assert_eq!(lines.next_line().await.unwrap().unwrap(), "MSG ahoj");
    wr.write_all(b"MSG cau\n").await.unwrap();
    wr.flush().await.unwrap();

    assert_eq!(fp_alice, bob_side.await.unwrap());
}

#[tokio::test]
async fn decline_through_relay() {
    let addr = start().await;
    let mut bob_listen = open(addr, "/listen?name=Bob").await.unwrap();
    let mut alice = open(addr, "/connect?from=Alice&to=Bob").await.unwrap();
    let id = invite_id(&read_line(&mut bob_listen).await);

    assert!(
        get(addr, &format!("/decline/{id}?name=Mallory"))
            .await
            .contains("404")
    );
    assert!(
        get(addr, &format!("/decline/{id}?name=Bob"))
            .await
            .contains("204")
    );
    assert_eq!(read_line(&mut alice).await, "DECLINE");
}

#[tokio::test]
async fn name_freed_after_disconnect() {
    let addr = start().await;
    let mut bob = open(addr, "/listen?name=Bob").await.unwrap();
    bob.shutdown().await.unwrap();
    drop(bob);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!get(addr, "/users").await.contains("Bob"));
    assert!(open(addr, "/listen?name=Bob").await.is_ok());
}
