use std::{
    collections::{BTreeSet, HashMap},
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

pub const DEFAULT_PORT: u16 = 52345;
const LOCAL_PORT_SPAN: u16 = 10;
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
    peers: Arc<Mutex<HashMap<SocketAddr, Peer>>>,
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
    pub fn upsert(&self, addr: SocketAddr, username: String) {
        let addr = canonical(addr, &local_ips());

        let mut peers = self.peers.lock().unwrap();
        let is_new_or_renamed = peers.get(&addr).is_none_or(|p| p.username != username);
        peers.insert(
            addr,
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

    pub fn snapshot(&self) -> Vec<(SocketAddr, Peer)> {
        let mut list: Vec<_> = self
            .peers
            .lock()
            .unwrap()
            .iter()
            .map(|(addr, p)| (*addr, p.clone()))
            .collect();
        list.sort_by_key(|(addr, _)| *addr);
        list
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

pub fn local_ips() -> Vec<IpAddr> {
    let mut ips: Vec<IpAddr> = if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .map(|iface| iface.ip())
        .filter(|ip| ip.is_ipv4())
        .collect();
    ips.push(IpAddr::V4(Ipv4Addr::LOCALHOST));
    ips.sort();
    ips.dedup();
    ips
}

fn canonical(addr: SocketAddr, locals: &[IpAddr]) -> SocketAddr {
    if addr.ip().is_loopback() || locals.contains(&addr.ip()) {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
    } else {
        addr
    }
}

pub async fn arp_neighbours() -> Vec<Ipv4Addr> {
    match Command::new("arp").arg("-an").output().await {
        Ok(out) => parse_arp(&String::from_utf8_lossy(&out.stdout)),
        // Silent: printing here would land in the middle of the menu screen.
        Err(_) => Vec::new(),
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

fn extra_targets() -> Vec<SocketAddr> {
    std::env::var("CLI_NET_PEERS")
        .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
        .unwrap_or_default()
}

fn build_targets(config: &Config, arp: &[Ipv4Addr], locals: &[IpAddr]) -> BTreeSet<SocketAddr> {
    let mut targets = BTreeSet::new();

    for ip in arp {
        targets.insert(SocketAddr::new(IpAddr::V4(*ip), config.port));
    }

    let local_ports = (DEFAULT_PORT..=DEFAULT_PORT.saturating_add(LOCAL_PORT_SPAN))
        .chain(std::iter::once(config.port));
    for port in local_ports {
        for ip in locals {
            targets.insert(SocketAddr::new(*ip, port));
        }
    }

    targets.extend(extra_targets());

    targets.retain(|t| !(t.port() == config.port && locals.contains(&t.ip())));
    targets
}

pub async fn scan_lan(config: Config, online: OnlineList) {
    online.prune();

    let locals = local_ips();
    let arp = arp_neighbours().await;
    let targets = build_targets(&config, &arp, &locals);

    let mut tasks = JoinSet::new();
    for addr in targets {
        let my_name = config.username.clone();
        let my_port = config.port;
        tasks.spawn(async move {
            say_hello(addr, &my_name, my_port)
                .await
                .map(|name| (addr, name))
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Ok(Some((addr, name))) = result {
            online.upsert(addr, name);
        }
    }
}

async fn say_hello(addr: SocketAddr, my_name: &str, my_port: u16) -> Option<String> {
    let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    stream
        .write_all(format!("HELLO <{my_name}> <{my_port}>\n").as_bytes())
        .await
        .ok()?;

    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    timeout(REPLY_TIMEOUT, reader.read_line(&mut line))
        .await
        .ok()?
        .ok()?;

    parse_greeting(&line, "HI").map(|(name, _)| name)
}

pub fn parse_greeting(msg: &str, keyword: &str) -> Option<(String, Option<u16>)> {
    let msg = msg.trim();
    if !msg.starts_with(keyword) {
        return None;
    }
    let parts = extract_brackets(msg);
    let name = parts.first()?.to_string();
    let port = parts.get(1).and_then(|p| p.parse().ok());
    Some((name, port))
}
