use regex::Regex;
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr},
};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::watch;

use if_addrs::Interface;

#[derive(Clone)]
pub struct Config {
    interfaces: Vec<Interface>,
    ipv4_addr: IpAddr,
    port: u16,
    username: String,
}

impl Default for Config {
    fn default() -> Self {
        let interfaces: Vec<Interface> = if_addrs::get_if_addrs()
            .unwrap_or_default()
            .into_iter()
            .filter(|iface| !iface.is_loopback() && iface.ip().is_ipv4())
            .collect();
        let ip_addr = interfaces
            .first()
            .map(|interface| interface.ip())
            .unwrap_or(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        Config {
            interfaces: interfaces,
            ipv4_addr: ip_addr,
            port: 52345,
            username: String::from("EMPTY"),
        }
    }
}

pub fn display_status(config: &Config) -> () {
    println!(
        "\0\x1b[2J\x1b[H
    ▗▄▄▖▗▖   ▗▄▄▄▖    ▗▖  ▗▖▗▄▄▄▖▗▄▄▄▖▗▖ ▗▖ ▗▄▖ ▗▄▄▖ ▗▖ ▗▖
   ▐▌   ▐▌     █      ▐▛▚▖▐▌▐▌     █  ▐▌ ▐▌▐▌ ▐▌▐▌ ▐▌▐▌▗▞▘
   ▐▌   ▐▌     █      ▐▌ ▝▜▌▐▛▀▀▘  █  ▐▌ ▐▌▐▌ ▐▌▐▛▀▚▖▐▛▚▖
   ▝▚▄▄▖▐▙▄▄▖▗▄█▄▖    ▐▌  ▐▌▐▙▄▄▖  █  ▐▙█▟▌▝▚▄▞▘▐▌ ▐▌▐▌ ▐▌



    ▗▄▄▖ ▗▄▖ ▗▖  ▗▖▗▖  ▗▖▗▖ ▗▖▗▖  ▗▖▗▄▄▄▖ ▗▄▄▖ ▗▄▖▗▄▄▄▖▗▄▖ ▗▄▄▖
   ▐▌   ▐▌ ▐▌▐▛▚▞▜▌▐▛▚▞▜▌▐▌ ▐▌▐▛▚▖▐▌  █  ▐▌   ▐▌ ▐▌ █ ▐▌ ▐▌▐▌ ▐▌
   ▐▌   ▐▌ ▐▌▐▌  ▐▌▐▌  ▐▌▐▌ ▐▌▐▌ ▝▜▌  █  ▐▌   ▐▛▀▜▌ █ ▐▌ ▐▌▐▛▀▚▖
   ▝▚▄▄▖▝▚▄▞▘▐▌  ▐▌▐▌  ▐▌▝▚▄▞▘▐▌  ▐▌▗▄█▄▖▝▚▄▄▖▐▌ ▐▌ █ ▝▚▄▞▘▐▌ ▐▌

                                                                "
    );
    println!("Your are now signed in as: {}", config.username);
    println!("Your setup configuration: ");
    println!("IP: {:>6}", config.ipv4_addr);
    println!("Port: {:>6}", config.port);
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

pub fn edit_name() -> () {
    todo!()
}

pub fn network_setup(config: &mut Config) -> () {
    'setup: loop {
        println!("\0\x1b[2J\x1b[HActual IP and Port configuration: ");
        println!("IP: {:>6}", config.ipv4_addr);
        println!("Port: {:>6}", config.port);
        std::io::stdout()
            .flush()
            .expect("Should flush terminal screen.");
        println!("Want to change interface: 1");
        println!("Want to change port: 2");

        let input = match return_input().trim().parse::<u8>() {
            Ok(val) => val,
            Err(_) => continue 'setup,
        };

        match input {
            1 => 'ip_addr: loop {
                println!("\0\x1b[2J\x1b[HChoose different network interface: ");
                for (index, int) in config.interfaces.iter().enumerate() {
                    println!("{} -> {}", index + 1, int.ip().to_string());
                }
                let new_addr: u8 = match return_input().trim().parse::<u8>() {
                    Ok(val) => val,
                    Err(_) => continue 'ip_addr,
                };
                match new_addr as usize {
                    choice if choice <= config.interfaces.len() && choice != 0 => {
                        let new_interface = config.interfaces[choice - 1].clone();
                        config.ipv4_addr = new_interface.ip();

                        break;
                    }
                    _ => println!("Invalid choice (1-{} available)", config.interfaces.len()),
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
            3 => {
                print!("Exiting network setup");
                return ();
            }
            _ => {
                println!("Invalid choide (1-3 available)");
            }
        }
    }
}

pub async fn active_connection(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    todo!()
}
pub async fn passive_connection(
    mut config_rx: watch::Receiver<Config>,
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
                    Ok((mut socket, _addr)) => {
                        tokio::spawn(async move {
                            let mut buffer = vec![0; 1024];
                            match socket.read(&mut buffer).await {
                                Ok(n) if n > 0 => {
                                    let received_message = String::from_utf8_lossy(&buffer[..n]);
                                    if received_message.contains("INVITE") {
                                        let host_name = extract_brackets(&received_message)
                                            .first()
                                            .copied()
                                            .unwrap_or("Unknown");
                                        println!("[ALARM] Invite from user: {}", host_name);
                                    }
                                }
                                Ok(_) => {}
                                Err(e) => eprintln!("Socket error: {e}"),
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
                        Err(e) => {
                            eprintln!("Failed to rebind on new address/port: {}. Will keep trying...", e);
                        }
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
