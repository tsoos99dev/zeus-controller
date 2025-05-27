mod app_config;
mod mqtt;

use mqtt::Request;
use tokio::{signal, sync::mpsc};

use tokio_util::{sync::CancellationToken, task::TaskTracker};

use app_config::Settings;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::new().expect("Failed to load app config");

    let tracker = TaskTracker::new();
    let token = CancellationToken::new();
    let cloned_token = token.clone();

    let mut interface = mqtt::Interface::new(settings.mqtt, cloned_token);

    let (tx, mut rx) = mpsc::channel(5);
    interface.add_handler("test", tx).await?;

    tracker.spawn(async move {
        loop {
            let Some(request) = rx.recv().await else {
                println!("Task receive channel closed");
                return;
            };

            let Request(data, resp_channel) = request;
            println!("{:?}", data);
            if let Err(e) = resp_channel.send(data) {
                eprintln!("Task response channel closed: {:?}", e);
            };
        }
    });

    tracker.spawn(async move {
        if let Err(e) = interface.run().await {
            eprintln!("Interface error: {:?}", e);
        };
    });

    tracker.close();

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Unable to listen for shutdown signal: {}", e);
        }
    }

    println!("Shutting down...");
    token.cancel();

    println!("Waiting for background tasks to finish...");
    tracker.wait().await;

    println!("Done.");

    Ok(())
}
