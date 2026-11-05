use tokio::sync::watch;
use utils::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();

    let (config_tx, config_rx) = watch::channel(config.clone());

    tokio::spawn(async move {
        if let Err(e) = passive_connection(config_rx).await {
            eprint!("Background listener error: {e}");
        }
    });

    'main: loop {
        display_status(&config);

        let input: u8 = match return_input().trim().parse::<u8>() {
            Ok(val) => val,
            Err(_) => continue 'main,
        };

        match input {
            1 => edit_name(),
            2 => {
                network_setup(&mut config);
                let _ = config_tx.send(config.clone());
            }
            3 => active_connection(&config).await?,
            _ => {
                println!("\0\x1b[2J\x1b[HThanks for using this app!");
                return Ok(());
            }
        }
    }
}
