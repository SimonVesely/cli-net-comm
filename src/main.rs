use std::io::Write;

struct Config {
    ipv4_addr: [u8; 4],
    port: u16,
    username: String,
    password: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            ipv4_addr: [127, 0, 0, 1],
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
    println!(
        "IP: {:>6}.{}.{}.{}",
        config.ipv4_addr[0], config.ipv4_addr[1], config.ipv4_addr[2], config.ipv4_addr[3]
    );
    println!("Port: {:>6}", config.port);
    std::io::stdout()
        .flush()
        .expect("Should flush terminal screen.");
}

fn auth_user() -> () {
    todo!()
}

fn network_setup() -> () {
    todo!()
}

fn lan_connection() -> () {
    todo!()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();

    'main: loop {
        let mut input: String = String::new();
        display_status(&config);

        std::io::stdin()
            .read_line(&mut input)
            .expect("Should read user input.");
        let input: u8 = match input.trim().parse::<u8>() {
            Ok(val) => val,
            Err(_) => continue 'main,
        };

        match input {
            1 => auth_user(),
            2 => network_setup(),
            3 => lan_connection(),
            _ => {
                return Ok(());
            }
        }
    }
}
