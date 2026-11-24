pub mod chat;
pub mod discovery;
pub mod relay;
pub mod tls;
pub mod ws;

use regex::Regex;
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

pub use chat::{InviteSource, Invites, active_connection, handle_pending_invite, is_in_chat};
pub use discovery::{DEFAULT_PORT, OnlineList, SCAN_INTERVAL, parse_greeting, scan_lan};
pub use relay::{DEFAULT_RELAY, RelayAddr, RelayState};

pub const DEFAULT_USERNAME: &str = "EMPTY";

pub static ON_MAIN_SCREEN: AtomicBool = AtomicBool::new(false);
static SCREEN_LOCK: Mutex<()> = Mutex::new(());

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

pub fn display_status(
    config: &Config,
    online: &OnlineList,
    invites: &Invites,
    relay: &RelayState,
) -> () {
    let _guard = SCREEN_LOCK.lock().unwrap();

    println!(
        "\0\x1b[2J\x1b[H
        ▜ ▘      ▗        ▌                 ▘    ▗
      ▛▘▐ ▌▄▖▛▌█▌▜▘▌▌▌▛▌▛▘▙▘▄▖▛▘▛▌▛▛▌▛▛▌▌▌▛▌▌▛▘▀▌▜▘▛▌▛▘
      ▙▖▐▖▌  ▌▌▙▖▐▖▚▚▘▙▌▌ ▛▖  ▙▖▙▌▌▌▌▌▌▌▙▌▌▌▌▙▖█▌▐▖▙▌▌

                                                                "
    );
    println!("Your are now signed in as: {}", config.username);
    println!("Your setup configuration: ");
    println!(
        "IP: {:>6}  {}",
        config.ipv4_addr,
        bind_label(&config.ipv4_addr)
    );
    println!("Port: {:>6}", config.port);

    let peers = online.snapshot();
    println!();
    println!(
        "── Online users ({}) ─────────────────────────",
        peers.len()
    );
    if peers.is_empty() {
        println!("   nobody found yet (press r to scan)");
    }
    for (addr, peer) in &peers {
        println!(
            "   ● {:<16} {:<21}  seen {}s ago",
            peer.username,
            addr,
            peer.last_seen.elapsed().as_secs()
        );
    }
    println!("───────────────────────────────────────────────");
    match &config.relay {
        Some(addr) => {
            let users = relay.users();
            println!();
            println!("── Relay {addr} — {}", relay.status());
            if users.is_empty() {
                println!("   nobody else online on relay");
            }
            for name in &users {
                println!("   ● {name}");
            }
            println!("───────────────────────────────────────────────");
        }
        None => println!("Relay: off"),
    }
    if let Some((from, addr)) = invites.peek() {
        println!();
        println!("   >>> Chat invite from {from} ({addr})  —  a -> accept, d -> decline");
    }
    println!();
    println!("1 -> Edit name   2 -> Network setup   3 -> Connect");
    println!("r -> Refresh / scan LAN                q -> Quit");
    print!("> ");
    std::io::stdout()
        .flush()
        .expect("Should flush terminal screen.");
}

pub fn return_input() -> String {
    let mut input: String = String::new();

    std::io::stdin()
        .read_line(&mut input)
        .expect("Should read user input.");

    input
}

pub fn edit_name(config: &mut Config) -> () {
    loop {
        println!("\0\x1b[2J\x1b[HCurrent name: {}", config.username);
        println!("Enter new name (empty = keep current): ");
        let input = return_input();
        let name = input.trim();

        if name.is_empty() {
            return;
        }
        if name.contains('<') || name.contains('>') {
            println!("Name can't contain '<' or '>'. Press Enter to try again.");
            return_input();
            continue;
        }
        config.username = name.to_string();
        return;
    }
}

pub fn network_setup(config: &mut Config) -> () {
    'setup: loop {
        println!("\0\x1b[2J\x1b[HActual IP and Port configuration: ");
        println!("IP: {:>6}", config.ipv4_addr);
        println!("Port: {:>6}", config.port);
        println!("Relay: {}", relay_label(config));
        std::io::stdout()
            .flush()
            .expect("Should flush terminal screen.");
        println!("Want to change interface: 1");
        println!("Want to change port: 2");
        println!("Want to change relay server: 3");
        println!("Back to main screen: 4");

        let input = match return_input().trim().parse::<u8>() {
            Ok(val) => val,
            Err(_) => continue 'setup,
        };

        match input {
            1 => 'ip_addr: loop {
                println!("\0\x1b[2J\x1b[HChoose different network interface: ");
                for (index, ip) in config.bind_options.iter().enumerate() {
                    println!("{} -> {} {}", index + 1, ip, bind_label(ip));
                }
                let new_addr: u8 = match return_input().trim().parse::<u8>() {
                    Ok(val) => val,
                    Err(_) => continue 'ip_addr,
                };
                match new_addr as usize {
                    choice if choice <= config.bind_options.len() && choice != 0 => {
                        config.ipv4_addr = config.bind_options[choice - 1];

                        break;
                    }
                    _ => println!("Invalid choice (1-{} available)", config.bind_options.len()),
                }
                continue 'setup;
            },
            2 => 'port: loop {
                println!("\0\x1b[2J\x1b[HChoose different port: ");
                let new_port: u16 = match return_input().trim().parse::<u16>() {
                    Ok(val) => val,
                    Err(_) => continue 'port,
                };
                config.port = new_port;
                continue 'setup;
            },
            3 => 'relay: loop {
                println!("\0\x1b[2J\x1b[HCurrent relay: {}", relay_label(config));
                println!(
                    "Enter relay URL (e.g. {DEFAULT_RELAY}), '-' to turn relay off, empty to keep: "
                );
                let input = return_input();
                match input.trim() {
                    "" => continue 'setup,
                    "-" => {
                        config.relay = None;
                        continue 'setup;
                    }
                    url => match RelayAddr::parse(url) {
                        Some(addr) => {
                            config.relay = Some(addr);
                            continue 'setup;
                        }
                        None => {
                            println!("Invalid URL. Press Enter to try again.");
                            return_input();
                            continue 'relay;
                        }
                    },
                }
            },
            4 => {
                print!("Exiting network setup");
                return ();
            }
            _ => {
                println!("Invalid choice (1-4 available)");
            }
        }
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

pub fn set_on_main_screen(value: bool) {
    ON_MAIN_SCREEN.store(value, Ordering::SeqCst);
}

pub fn is_on_main_screen() -> bool {
    ON_MAIN_SCREEN.load(Ordering::SeqCst)
}

fn relay_label(config: &Config) -> String {
    config
        .relay
        .as_ref()
        .map_or_else(|| "off".to_string(), |addr| addr.to_string())
}

fn bind_label(ip: &IpAddr) -> &'static str {
    if ip.is_unspecified() {
        "(all interfaces)"
    } else if ip.is_loopback() {
        "(loopback - this machine only)"
    } else {
        ""
    }
}

fn extract_brackets(text: &str) -> Vec<&str> {
    let re = Regex::new(r"<([^>]+)>").unwrap();

    re.captures_iter(text)
        .map(|cap| cap.get(1).unwrap().as_str())
        .collect()
}
