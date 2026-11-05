use utils::*;

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
            3 => lan_connection(&config).await?,
            _ => {
                println!("\0\x1b[2J\x1b[HThanks for using this app!");
                return Ok(());
            }
        }
    }
}
