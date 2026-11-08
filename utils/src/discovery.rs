use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use regex::Regex;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::Command,
    sync::watch,
    task::JoinSet,
    time::timeout,
};

use crate::{Config, extract_brackets};

pub const SCAN_INTERVAL: Duration = Duration::from_secs(30);
const PEER_TTL: Duration = Duration::from_secs(90);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(800);
const REPLY_TIMEOUT: Duration = Duration::from_millis(1500);

#[derive(Clone, Debug)]
pub struct Peer {
    pub username: String,
    pub last_seen: Instant,
}

#[derive(Clone)]
pub struct OnlineList {
    peers: Arc<Mutex<HashMap<IpAddr, Peer>>>,
    changed: Arc<watch::Sender<u64>>,
}

impl Default for OnlineList {
    fn default() -> Self {
        let (tx, _rx) = watch::channel(0);
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            changed: Arc::new(tx),
        }
    }
}

impl OnlineList {
    pub fn upsert(&self, ip: IpAddr, username: String) {
        let mut peers = self.peers.lock().unwrap();
        let is_new_or_renamed = peers.get(&ip).is_none_or(|p| p.username != username);
        peers.insert(
            ip,
            Peer {
                username,
                last_seen: Instant::now(),
            },
        );
        drop(peers);
        if is_new_or_renamed {
            self.changed.send_modify(|v| *v += 1);
        }
    }

    pub fn prune(&self) {
        let mut peers = self.peers.lock().unwrap();
        let before = peers.len();
        peers.retain(|_, p| p.last_seen.elapsed() < PEER_TTL);
        let removed = before != peers.len();
        drop(peers);
        if removed {
            self.changed.send_modify(|v| *v += 1);
        }
    }

    pub fn snapshot(&self) -> Vec<(IpAddr, Peer)> {
        let mut list: Vec<_> = self
            .peers
            .lock()
            .unwrap()
            .iter()
            .map(|(ip, p)| (*ip, p.clone()))
            .collect();
        list.sort_by_key(|(ip, _)| *ip);
        list
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

pub async fn arp_neighbours() -> Vec<Ipv4Addr> {
    match Command::new("arp").arg("-an").output().await {
        Ok(out) => parse_arp(&String::from_utf8_lossy(&out.stdout)),
        Err(e) => {
            eprintln!("Failed to run `arp -an`: {e}");
            Vec::new()
        }
    }
}

fn parse_arp(text: &str) -> Vec<Ipv4Addr> {
    let re = Regex::new(r"\((\d{1,3}(?:\.\d{1,3}){3})\)").unwrap();

    let mut ips: Vec<Ipv4Addr> = text
        .lines()
        .filter(|l| !l.contains("incomplete") && !l.contains("ff:ff:ff:ff:ff:ff"))
        .filter_map(|l| re.captures(l)?.get(1)?.as_str().parse().ok())
        .filter(|ip: &Ipv4Addr| {
            !ip.is_multicast() && !ip.is_broadcast() && !ip.is_loopback() && ip.octets()[3] != 255
        })
        .collect();

    ips.sort();
    ips.dedup();
    ips
}

pub async fn scan_lan(config: Config, online: OnlineList) {
    online.prune();

    let mut tasks = JoinSet::new();
    for ip in arp_neighbours().await {
        let ip = IpAddr::V4(ip);
        if ip == config.ipv4_addr {
            continue;
        }
        // Assumes everyone runs the app on the same port as we do.
        let addr = SocketAddr::new(ip, config.port);
        let my_name = config.username.clone();
        tasks.spawn(async move { say_hello(addr, &my_name).await.map(|name| (ip, name)) });
    }

    while let Some(result) = tasks.join_next().await {
        if let Ok(Some((ip, name))) = result {
            online.upsert(ip, name);
        }
    }
}

async fn say_hello(addr: SocketAddr, my_name: &str) -> Option<String> {
    let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    stream
        .write_all(format!("HELLO <{my_name}>\n").as_bytes())
        .await
        .ok()?;

    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    timeout(REPLY_TIMEOUT, reader.read_line(&mut line))
        .await
        .ok()?
        .ok()?;

    parse_greeting(&line, "HI")
}

pub fn parse_greeting(msg: &str, keyword: &str) -> Option<String> {
    let msg = msg.trim();
    if !msg.starts_with(keyword) {
        return None;
    }
    extract_brackets(msg).first().map(|s| s.to_string())
}
