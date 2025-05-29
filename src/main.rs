mod app_config;
mod mqtt;
mod relay;
mod service;

use std::time::Duration;

use service::Service;
use tokio::signal;

use tokio_util::{sync::CancellationToken, task::TaskTracker};

use app_config::Settings;

const DEFAULT_LOGGING: &str = "info";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log_config = std::env::var("RUST_LOG")
        .ok()
        .and_then(|l| {
            if l.is_empty() {
                {
                    None
                }
            } else {
                Some(l)
            }
        })
        .unwrap_or_else(|| DEFAULT_LOGGING.to_owned());
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(log_config)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let settings = Settings::new()?;

    let tracker = TaskTracker::new();
    let token = CancellationToken::new();
    let cloned_token = token.clone();

    let mut mqtt_interface = mqtt::Interface::new(settings.mqtt, cloned_token)?;
    let relay_interface = relay::Interface::new(
        &settings.relay.device,
        settings.relay.unit_id,
        settings.relay.baud_rate,
        Duration::from_secs(settings.relay.timeout.0),
    );

    let relay_proxy = relay_interface.spawn();

    mqtt_interface
        .add_handler("relay/command", move |request| {
            let proxy = relay_proxy.clone();
            async move { proxy.send(request).await }
        })
        .await?;

    tracker.spawn(async move {
        if let Err(e) = mqtt_interface.run().await {
            tracing::error!("Interface error: {e:?}");
        };
    });

    tracker.close();

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(e) => {
            tracing::error!("Unable to listen for shutdown signal: {e}");
        }
    }

    tracing::debug!("Shutting down...");
    token.cancel();

    tracing::debug!("Waiting for background tasks to finish...");
    tracker.wait().await;

    tracing::debug!("Done.");

    Ok(())
}
