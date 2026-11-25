pub mod chat;
pub mod discovery;
pub mod relay;
pub mod tls;
pub mod ws;

use regex::Regex;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

pub use chat::{InviteSource, Invites, is_in_chat};
pub use discovery::{DEFAULT_PORT, OnlineList, SCAN_INTERVAL, parse_greeting, scan_lan};
pub use relay::{DEFAULT_RELAY, RelayAddr, RelayState};

pub const DEFAULT_USERNAME: &str = "EMPTY";

#[derive(Clone)]
pub struct Config {
    bind_options: Vec<IpAddr>,
    ipv4_addr: IpAddr,
    port: u16,
    username: String,
    relay: Option<RelayAddr>,
}

impl Default for Config {
    fn default() -> Self {
        let mut bind_options: Vec<IpAddr> = if_addrs::get_if_addrs()
            .unwrap_or_default()
            .into_iter()
            .filter(|iface| !iface.is_loopback() && iface.ip().is_ipv4())
            .map(|iface| iface.ip())
            .collect();
        let ip_addr = bind_options
            .first()
            .copied()
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        bind_options.push(IpAddr::V4(Ipv4Addr::LOCALHOST));
        bind_options.push(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        Config {
            bind_options,
            ipv4_addr: ip_addr,
            port: DEFAULT_PORT,
            username: String::from(DEFAULT_USERNAME),
            relay: RelayAddr::parse(DEFAULT_RELAY),
        }
    }
}

impl Config {
    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn has_name(&self) -> bool {
        self.username != DEFAULT_USERNAME
    }

    pub fn ip(&self) -> IpAddr {
        self.ipv4_addr
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn relay(&self) -> Option<&RelayAddr> {
        self.relay.as_ref()
    }

    pub fn bind_options(&self) -> &[IpAddr] {
        &self.bind_options
    }

    pub fn set_username(&mut self, name: &str) -> Result<(), &'static str> {
        let name = name.trim();
        if name.is_empty() {
            return Err("name can't be empty");
        }
        if name.contains(['<', '>']) {
            return Err("name can't contain '<' or '>'");
        }
        if name.chars().count() > 32 {
            return Err("name can have at most 32 characters");
        }
        self.username = name.to_string();
        Ok(())
    }

    pub fn set_ip(&mut self, ip: IpAddr) {
        self.ipv4_addr = ip;
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    pub fn set_relay(&mut self, relay: Option<RelayAddr>) {
        self.relay = relay;
    }
}

pub async fn passive_connection(
    mut config_rx: watch::Receiver<Config>,
    online: OnlineList,
    invites: Invites,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut current_config = config_rx.borrow().clone();
    let mut listener = TcpListener::bind(format!(
        "{}:{}",
        current_config.ipv4_addr, current_config.port
    ))
    .await?;

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((mut socket, addr)) => {
                        let (my_name, my_port, has_name) = {
                            let cfg = config_rx.borrow();
                            (cfg.username.clone(), cfg.port, cfg.has_name())
                        };
                        let online = online.clone();
                        let invites = invites.clone();

                        tokio::spawn(async move {
                            if !has_name {
                                return;
                            }
                            let mut buffer = vec![0; 1024];
                            match socket.read(&mut buffer).await {
                                Ok(n) if n > 0 => {
                                    let received_message = String::from_utf8_lossy(&buffer[..n]);

                                    if let Some((their_name, their_port)) = parse_greeting(&received_message, "HELLO") {
                                        let reply = format!("HI <{my_name}>\n");
                                        let _ = socket.write_all(reply.as_bytes()).await;
                                        let their_addr = SocketAddr::new(addr.ip(), their_port.unwrap_or(my_port));
                                        online.upsert(their_addr, their_name, false);
                                    } else if received_message.starts_with("INVITE") {
                                        let host_name = extract_brackets(&received_message)
                                            .first()
                                            .map(|s| s.to_string())
                                            .unwrap_or_else(|| "Unknown".into());

                                        if is_in_chat() {
                                            let _ = socket.write_all(b"BUSY\n").await;
                                        } else {
                                            match invites.offer(host_name, addr.to_string(), InviteSource::Lan(socket)) {
                                                Ok(()) => online.touch(),
                                                Err(InviteSource::Lan(mut socket)) => {
                                                    let _ = socket.write_all(b"BUSY\n").await;
                                                }
                                                Err(_) => {}
                                            }
                                        }
                                    }
                                }
                                Ok(_) => {}
                                Err(_) => {}
                            }
                        });
                    }
                    Err(_) => continue,
                }
            }

            changed_result = config_rx.changed() => {
                if changed_result.is_err() {
                    break;
                }
                let new_config = config_rx.borrow().clone();

                if new_config.ipv4_addr != current_config.ipv4_addr || new_config.port != current_config.port {
                    current_config = new_config;

                    match TcpListener::bind(format!("{}:{}", current_config.ipv4_addr, current_config.port)).await {
                        Ok(new_listener) => {
                            listener = new_listener;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    }
    Ok(())
}

fn extract_brackets(text: &str) -> Vec<&str> {
    let re = Regex::new(r"<([^>]+)>").unwrap();

    re.captures_iter(text)
        .map(|cap| cap.get(1).unwrap().as_str())
        .collect()
}
