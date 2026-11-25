use std::net::SocketAddr;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr: SocketAddr = match std::env::var("PORT") {
        Ok(port) => SocketAddr::from(([0, 0, 0, 0], port.parse().expect("PORT must be a number"))),
        Err(_) => std::env::var("RELAY_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .expect("RELAY_ADDR must be ip:port"),
    };
    let settings = relay::Settings::from_env();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!(
        "relay listening on {} (trust_proxy={}, public_users={})",
        listener.local_addr()?,
        settings.trust_proxy,
        settings.public_users
    );
    tokio::select! {
        result = relay::serve(listener, settings) => result,
        _ = shutdown() => Ok(()),
    }
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
