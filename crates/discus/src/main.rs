use clap::Parser;
use discus::{Config, Server};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config = Config::parse();
    // The backend provider becomes the process-wide rustls default before anything builds a TLS
    // config. Under tls-openssl, TLS to the OTLP collector needs it.
    discus::security::tls::install_default();
    // Logging, metrics, and traces. A flag that needs a missing build feature stops the start.
    let _telemetry = match discus::telemetry::init(&config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("discus: {e}");
            std::process::exit(2);
        }
    };
    tracing::info!(
        build = discus::build::BUILD_ID,
        features = discus::build::FEATURES.join(","),
        tls_backend = discus::build::TLS_BACKEND,
        trust_roots = discus::build::TRUST_ROOTS,
        "starting"
    );
    let server = Server::bind(config).await?;
    discus::telemetry::observe_store(server.node());
    tracing::info!(client = %server.local_addr(), admin = %server.admin_addr(), "bound");

    let shutdown = CancellationToken::new();
    let on_signal = shutdown.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("shutdown signal received");
        on_signal.cancel();
    });
    server.run(shutdown).await
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        return std::future::pending().await;
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
