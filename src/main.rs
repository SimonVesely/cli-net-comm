use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr},
};

use if_addrs::Interface;

struct Config {
    interfaces: Vec<Interface>,
    ipv4_addr: IpAddr,
    port: u16,
    username: String,
    password: String,
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
            password: String::from(""),
        }
    }
}

fn display_status(config: &Config) -> () {
    println!(
        "
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

fn return_input() -> String {
    let mut input: String = String::new();

    std::io::stdin()
        .read_line(&mut input)
        .expect("Should read user input.");

    input
}

fn auth_user() -> () {
    todo!()
}

fn network_setup(config: &mut Config) -> () {
    'setup: loop {
        println!("Actual IP and Port configuration: ");
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
                println!("Choose different network interface: ");
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
                println!("Choode different port: ");
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

fn lan_connection() -> () {
    todo!()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();

    'main: loop {
        display_status(&config);

        let input: u8 = match return_input().trim().parse::<u8>() {
            Ok(val) => val,
            Err(_) => continue 'main,
        };

        match input {
            1 => auth_user(),
            2 => network_setup(&mut config),
            3 => lan_connection(),
            _ => {
                return Ok(());
            }
        }
    }
}
