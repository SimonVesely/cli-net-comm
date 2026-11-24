mod input;
mod tui;

use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, watch};
use utils::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let online = OnlineList::default();
    let invites = Invites::default();
    let relay_state = RelayState::default();
    let scan_now = Arc::new(Notify::new());
    let listener_error = Arc::new(Mutex::new(None));

    let (config_tx, config_rx) = watch::channel(config.clone());

    {
        let config_rx = config_rx.clone();
        let online = online.clone();
        let invites = invites.clone();
        let listener_error = listener_error.clone();
        tokio::spawn(async move {
            let mut changes = config_rx.clone();
            loop {
                changes.borrow_and_update();
                *listener_error.lock().unwrap() = None;
                match passive_connection(config_rx.clone(), online.clone(), invites.clone()).await {
                    Ok(()) => break,
                    Err(e) => *listener_error.lock().unwrap() = Some(format!("listener: {e}")),
                }
                if changes.changed().await.is_err() {
                    break;
                }
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
        let mut config_rx = config_rx.clone();
        let online = online.clone();
        let scan_now = scan_now.clone();
        tokio::spawn(async move {
            loop {
                let cfg = config_rx.borrow_and_update().clone();
                if cfg.has_name() {
                    scan_lan(cfg, online.clone()).await;
                } else {
                    online.clear();
                }

                tokio::select! {
                    _ = tokio::time::sleep(SCAN_INTERVAL) => {}
                    _ = scan_now.notified() => {}
                    changed = config_rx.changed() => if changed.is_err() { break },
                }
            }
        });
    }

    tui::run(tui::App {
        config,
        config_tx,
        online,
        invites,
        relay_state,
        scan_now,
        listener_error,
    })
    .await?;
    Ok(())
}
