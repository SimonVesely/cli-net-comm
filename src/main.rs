use std::sync::Arc;

use tokio::sync::{Notify, watch};
use utils::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    let online = OnlineList::default();
    let scan_now = Arc::new(Notify::new());

    let (config_tx, config_rx) = watch::channel(config.clone());

    {
        let config_rx = config_rx.clone();
        let online = online.clone();
        tokio::spawn(async move {
            if let Err(e) = passive_connection(config_rx, online).await {
                eprint!("Background listener error: {e}");
            }
        });
    }

    {
        let config_rx = config_rx.clone();
        let online = online.clone();
        let scan_now = scan_now.clone();
        tokio::spawn(async move {
            loop {
                let cfg = config_rx.borrow().clone();
                scan_lan(cfg, online.clone()).await;

                tokio::select! {
                    _ = tokio::time::sleep(SCAN_INTERVAL) => {}
                    _ = scan_now.notified() => {}
                }
            }
        });
    }

    {
        let config_rx = config_rx.clone();
        let online = online.clone();
        let mut changes = online.subscribe();
        tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                if is_on_main_screen() {
                    let cfg = config_rx.borrow().clone();
                    display_status(&cfg, &online);
                }
            }
        });
    }

    'main: loop {
        display_status(&config, &online);
        set_on_main_screen(true);
        let input = return_input();
        set_on_main_screen(false);

        match input.trim() {
            "1" => {
                edit_name(&mut config);
                let _ = config_tx.send(config.clone());
            }
            "2" => {
                network_setup(&mut config);
                let _ = config_tx.send(config.clone());
            }
            "3" => active_connection(&config).await?,
            "r" | "R" => {
                scan_now.notify_one();
            }
            "q" | "Q" => {
                println!("\0\x1b[2J\x1b[HThanks for using this app!");
                return Ok(());
            }
            _ => continue 'main,
        }
    }
}
