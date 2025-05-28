mod app_config;
mod mqtt;
mod ops;
mod relay;
mod service;

use service::Service;
use tokio::signal;

use tokio_util::{sync::CancellationToken, task::TaskTracker};

use app_config::Settings;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::new().expect("Failed to load app config");

    let tracker = TaskTracker::new();
    let token = CancellationToken::new();
    let cloned_token = token.clone();

    let mut mqtt_interface = mqtt::Interface::new(settings.mqtt, cloned_token);
    let relay_interface = relay::Interface::new("/dev/ttyS1", 1, 9600);

    let relay_proxy = relay_interface.spawn();

    mqtt_interface
        .add_handler("relay/command", relay_proxy)
        .await?;

    tracker.spawn(async move {
        if let Err(e) = mqtt_interface.run().await {
            eprintln!("Interface error: {e:?}");
        };
    });

    tracker.close();

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Unable to listen for shutdown signal: {e}");
        }
    }

    println!("Shutting down...");
    token.cancel();

    println!("Waiting for background tasks to finish...");
    tracker.wait().await;

    println!("Done.");

    Ok(())
}
