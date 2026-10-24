//! Entry point for `http-multicaster`.

use std::{net::Ipv4Addr, process::ExitCode};

use http_multicaster::{
    config::Config,
    multicaster::{self, AppState},
    telemetry,
};
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            telemetry::init("info", http_multicaster::config::LogFormat::Text);
            error!(error = %err, "Failed to load configuration");
            return ExitCode::FAILURE;
        }
    };

    telemetry::init(&config.log_level, config.log_format);

    let port = config.port;
    let endpoints = config.target_endpoints.clone();
    let state = match AppState::new(config) {
        Ok(state) => state,
        Err(err) => {
            error!(error = %err, "Failed to initialize HTTP client");
            return ExitCode::FAILURE;
        }
    };

    let listener = match TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await {
        Ok(listener) => listener,
        Err(err) => {
            error!(port = port, error = %err, "Failed to bind TCP listener");
            return ExitCode::FAILURE;
        }
    };

    info!(
        port = port,
        targets = ?endpoints,
        "http-multicaster listening"
    );

    let app = multicaster::router(state);
    if let Err(err) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!(error = %err, "Server exited with error");
        return ExitCode::FAILURE;
    }

    info!("http-multicaster stopped cleanly");
    ExitCode::SUCCESS
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!(error = %err, "Failed to listen for Ctrl-C");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                error!(error = %err, "Failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("Received Ctrl-C, shutting down"),
        () = terminate => info!("Received SIGTERM, shutting down"),
    }
}

