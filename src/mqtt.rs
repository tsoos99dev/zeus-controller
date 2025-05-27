use std::{collections::HashMap, string::FromUtf8Error, sync::Arc, time::Duration};

use bytes::Bytes;
use rumqttc::{
    tokio_rustls::rustls::{
        self,
        pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
        ClientConfig, RootCertStore,
    },
    v5::{
        mqttbytes::{
            v5::{Publish, PublishProperties},
            QoS,
        },
        ClientError, EventLoop,
    },
    Outgoing,
};
use rumqttc::{
    v5::{mqttbytes::v5::Packet, AsyncClient, Event, MqttOptions},
    Transport,
};
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio_rustls::rustls::pki_types;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::app_config::{MQTTSettings, TLSSettings};

// Number of messages allowed in the async client's buffer
const CLIENT_CHANNEL_CAPACITY: usize = 20;

// Amount of time to wait before advancing the loop after an error
const RETRY_DELAY: u64 = 1;

// Max number of requests allowed to be processed concurrently
const REQUEST_CONCURRENCY: usize = 5;

// Max number of seconds to wait for an available handler before the packet is dropped
const REQUEST_WAIT_PERIOD: u64 = 2;

#[derive(thiserror::Error, Debug)]
enum TLSSettingError {
    #[error("Failed to load root certificate")]
    RootCertLoad(#[source] pki_types::pem::Error),

    #[error("Failed to load client certificate")]
    ClientCertLoad(#[source] pki_types::pem::Error),

    #[error("Failed to parse client certificate")]
    ClientCertParse(#[source] pki_types::pem::Error),

    #[error("Failed to load client key")]
    ClientKeyLoad(#[source] pki_types::pem::Error),

    #[error("Failed to build root certificate store")]
    RootCertificateStore(#[source] rustls::Error),

    #[error("Failed to create client configuration")]
    ClientConfig(#[source] rustls::Error),
}

#[derive(thiserror::Error, Debug)]
enum RequestError {
    #[error("Too many requests")]
    TooManyRequests,

    #[error("Invalid topic")]
    InvalidTopic(#[source] FromUtf8Error),

    #[error("Missing properties")]
    MissingProperties,

    #[error("Missing response topic")]
    MissingResponseTopic,

    #[error("Missing correlation data")]
    MissingCorrelationData,

    #[error("Invalid payload format")]
    InvalidPayloadFormat(#[from] rmp_serde::decode::Error),

    #[error("Handler dropped request")]
    HandlerDroppedRequest,

    #[error("Handler not listening")]
    HandlerNotListening,
}

#[derive(thiserror::Error, Debug)]
pub enum InterfaceError {
    #[error("Couldn't disconnect cleanly")]
    DisconnectFailed(#[from] ClientError),

    #[error("Executor already running")]
    AlreadyRunning,
}

fn set_tls_settings(
    mqttoptions: &mut MqttOptions,
    tls_settings: TLSSettings,
) -> Result<&mut MqttOptions, TLSSettingError> {
    let mut root_cert_store = RootCertStore::empty();
    let root_cert = CertificateDer::from_pem_file(tls_settings.ca_cert)
        .map_err(TLSSettingError::RootCertLoad)?;
    let client_certs = CertificateDer::pem_file_iter(tls_settings.certfile)
        .map_err(TLSSettingError::ClientCertLoad)?
        .map(|cert| cert.map_err(TLSSettingError::ClientCertParse))
        .collect::<Result<Vec<_>, TLSSettingError>>()?;

    let client_key = PrivateKeyDer::from_pem_file(tls_settings.keyfile)
        .map_err(TLSSettingError::ClientKeyLoad)?;

    root_cert_store
        .add(root_cert)
        .map_err(TLSSettingError::RootCertificateStore)?;

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(TLSSettingError::ClientConfig)?;

    mqttoptions.set_transport(Transport::tls_with_config(client_config.into()));
    Ok(mqttoptions)
}

#[derive(Debug)]
pub struct Request(pub Bytes, pub oneshot::Sender<Bytes>);
type RequestHandle = mpsc::Sender<Request>;
type RequestHandleRegistry = Arc<Mutex<HashMap<String, RequestHandle>>>;

pub struct Interface {
    client: AsyncClient,
    executor: Option<InterfaceExecutor>,
    request_handle_registry: RequestHandleRegistry,
    external_cancellation_token: CancellationToken,
    internal_cancellation_token: CancellationToken,
}

impl Interface {
    pub fn new(client_settings: MQTTSettings, token: CancellationToken) -> Self {
        let mut options = MqttOptions::new("", client_settings.host, client_settings.port);
        options.set_connection_timeout(client_settings.connection_timeout.0);

        if let Some(tls_settings) = client_settings.tls {
            set_tls_settings(&mut options, tls_settings).unwrap();
        }

        let (client, eventloop) = AsyncClient::new(options.clone(), CLIENT_CHANNEL_CAPACITY);

        let internal_cancellation_token = CancellationToken::new();

        let request_handle_registry = Arc::new(Mutex::new(HashMap::new()));
        let executor = InterfaceExecutor {
            eventloop,
            request_handler: RequestHandler::new(client.clone(), request_handle_registry.clone()),
            cancellation_token: internal_cancellation_token.clone(),
        };

        Interface {
            client,
            executor: Some(executor),
            request_handle_registry,
            external_cancellation_token: token,
            internal_cancellation_token,
        }
    }

    pub async fn run(&mut self) -> Result<(), InterfaceError> {
        let tracker = TaskTracker::new();

        let internal_cancellation_token_clone = self.internal_cancellation_token.clone();
        let mut executor = self.executor.take().ok_or(InterfaceError::AlreadyRunning)?;

        tracker.spawn(async move {
            tokio::select! {
                _ = executor.poll() => {},
                _ = internal_cancellation_token_clone.cancelled() => {}
            }
        });

        self.external_cancellation_token.cancelled().await;
        tracker.close();

        let state = self
            .client
            .disconnect()
            .await
            .map_err(InterfaceError::DisconnectFailed)
            .inspect_err(|_| self.internal_cancellation_token.cancel());

        tracker.wait().await;
        state
    }

    pub async fn add_handler(&self, topic: &str, handle: RequestHandle) -> Result<(), ClientError> {
        let mut r = self.request_handle_registry.lock().await;
        r.insert(topic.into(), handle);
        self.client.subscribe(topic, QoS::AtMostOnce).await
    }

    pub async fn publish(&self, topic: &str, data: Bytes) -> Result<(), ClientError> {
        self.client
            .publish_bytes(topic, QoS::AtMostOnce, false, data)
            .await
    }
}

struct InterfaceExecutor {
    eventloop: EventLoop,
    request_handler: RequestHandler,
    cancellation_token: CancellationToken,
}

impl InterfaceExecutor {
    async fn poll(&mut self) {
        loop {
            let event = self.eventloop.poll().await;
            match event {
                Ok(notification) => {
                    println!("Received = {:?}", notification);
                    match notification {
                        Event::Incoming(Packet::ConnAck(packet)) => {
                            println!("Client connected: {:?}", packet.code);
                        }
                        Event::Incoming(Packet::Disconnect(packet)) => {
                            println!("Client disconnected: {:?}", packet.reason_code);
                        }
                        Event::Outgoing(Outgoing::Disconnect) => {
                            println!("Closing...");
                            self.cancellation_token.cancel();
                        }
                        Event::Incoming(Packet::Publish(packet)) => {
                            if let Err(e) = self.request_handler.accept(packet).await {
                                eprintln!("Failed to process request: {:?}", e);
                            };
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    eprintln!("Error = {e:?}");
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY)).await;
                }
            }
        }
    }
}

#[derive(Clone)]
struct RequestHandler {
    client: AsyncClient,
    handles: RequestHandleRegistry,
    request_semaphore: Arc<Semaphore>,
}

impl RequestHandler {
    fn new(client: AsyncClient, handles: RequestHandleRegistry) -> Self {
        RequestHandler {
            client,
            handles,
            request_semaphore: Arc::new(Semaphore::new(REQUEST_CONCURRENCY)),
        }
    }

    async fn accept(&self, packet: Publish) -> Result<(), RequestError> {
        let permit = tokio::time::timeout(
            Duration::from_secs(REQUEST_WAIT_PERIOD),
            self.request_semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| RequestError::TooManyRequests)?;

        let topic: String =
            String::from_utf8(packet.topic.into()).map_err(RequestError::InvalidTopic)?;
        let properties = packet.properties.ok_or(RequestError::MissingProperties)?;

        let response_topic = properties
            .response_topic
            .ok_or(RequestError::MissingResponseTopic)?;

        let correlation_data = properties
            .correlation_data
            .ok_or(RequestError::MissingCorrelationData)?;

        let (resp_tx, resp_rx) = oneshot::channel();

        let handles = self.handles.lock().await;
        let Some(request_handle) = handles.get(&topic) else {
            return Ok(());
        };

        let request_handle = request_handle.clone();

        let client = self.client.clone();

        tokio::spawn(async move {
            if let Err(e) = request_handle
                .send(Request(packet.payload, resp_tx))
                .await
                .map_err(|_| RequestError::HandlerNotListening)
            {
                eprintln!("Request error: {:?}", e);
                drop(permit);
                return;
            }

            let Ok(response) = resp_rx.await else {
                eprintln!("Request error: {:?}", RequestError::HandlerDroppedRequest);
                drop(permit);
                return;
            };

            let properties = PublishProperties {
                correlation_data: Some(correlation_data),
                ..Default::default()
            };

            if let Err(e) = client
                .publish_bytes_with_properties(
                    response_topic,
                    QoS::AtMostOnce,
                    false,
                    response,
                    properties,
                )
                .await
            {
                eprintln!("Failed to publish response: {:?}", e);
            };

            drop(permit);
        });

        Ok(())
    }
}
