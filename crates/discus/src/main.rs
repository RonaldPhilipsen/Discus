use discus::{Config, Server};
use tokio_util::sync::CancellationToken;

// Every request allocates for parsing, frames, and replies. The system allocator took 10% to 16%
// of the CPU under load with glibc, and the release image links musl, whose allocator is slower.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::io::Result<()> {
    let config = match Config::load_from(std::env::args_os()) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("discus: {e}");
            std::process::exit(2);
        }
    };
    let target = discus::priority::data_thread_target(config.data_nice);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(move || {
            if let Some(t) = target {
                discus::priority::set_current_thread_nice_recorded(t);
            }
        })
        .build()?;
    runtime.block_on(run(config, target))
}

async fn run(mut config: Config, nice_target: Option<i32>) -> std::io::Result<()> {
    // The node ID is fixed before telemetry starts, so the resource carries the real ID.
    if let Err(e) = config.resolve_identity() {
        eprintln!("discus: {e}");
        std::process::exit(2);
    }
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
    let _profiling = match discus::profiling::start(&config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("discus: {e}");
            std::process::exit(2);
        }
    };
    if let Some(e) = discus::priority::first_failure() {
        tracing::warn!(
            error = %e,
            data_nice = config.data_nice,
            target_nice = ?nice_target,
            "could not lower the data runtime priority"
        );
    }
    tracing::info!(
        build = discus::build::BUILD_ID,
        features = discus::build::FEATURES.join(","),
        tls_backend = discus::build::TLS_BACKEND,
        trust_roots = discus::build::TRUST_ROOTS,
        max_memory = config.max_memory,
        "starting"
    );
    for key in discus::settings::KEYS {
        let source = if config.cli_set.contains(key.flag) {
            "command line"
        } else if config.file_set.contains(key.flag) {
            "file"
        } else {
            continue;
        };
        tracing::info!(setting = key.flag, source, "setting source");
    }
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
