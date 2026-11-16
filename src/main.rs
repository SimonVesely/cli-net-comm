use std::sync::Arc;

use tokio::sync::{Notify, watch};
use utils::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    let online = OnlineList::default();
    let invites = Invites::default();
    let relay_state = RelayState::default();
    let scan_now = Arc::new(Notify::new());

    let (config_tx, config_rx) = watch::channel(config.clone());

    {
        let config_rx = config_rx.clone();
        let online = online.clone();
        let invites = invites.clone();
        tokio::spawn(async move {
            if let Err(e) = passive_connection(config_rx, online, invites).await {
                eprint!("Background listener error: {e}");
            }
        });
    }

    {
        let config_rx = config_rx.clone();
        let online = online.clone();
        let invites = invites.clone();
        let relay_state = relay_state.clone();
        tokio::spawn(utils::relay::run(config_rx, online, invites, relay_state));
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
        let invites = invites.clone();
        let relay_state = relay_state.clone();
        let mut changes = online.subscribe();
        tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                if is_on_main_screen() {
                    let cfg = config_rx.borrow().clone();
                    display_status(&cfg, &online, &invites, &relay_state);
                }
            }
        });
    }

    'main: loop {
        display_status(&config, &online, &invites, &relay_state);
        set_on_main_screen(true);
        let input = tokio::task::block_in_place(return_input);
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
            "3" => active_connection(&config, &online, &relay_state).await,
            "a" | "A" => handle_pending_invite(&config, &invites, true).await,
            "d" | "D" => handle_pending_invite(&config, &invites, false).await,
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
