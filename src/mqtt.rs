use std::{
    collections::HashMap, fmt::Debug, future::Future, pin::Pin, string::FromUtf8Error, sync::Arc,
    time::Duration,
};

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
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Semaphore},
    time::error::Elapsed,
};
use tokio_rustls::rustls::pki_types;
use tokio_util::sync::CancellationToken;

use crate::app_config::{MQTTSettings, TLSSettings};

// Number of messages allowed in the async client's buffer
const CLIENT_CHANNEL_CAPACITY: usize = 20;

// Amount of time to wait before advancing the loop after an error
const RETRY_DELAY: u64 = 1;

// Max number of requests allowed to be processed concurrently
const REQUEST_CONCURRENCY: usize = 5;

// Max number of seconds to wait for an available handler before the packet is dropped
const REQUEST_WAIT_PERIOD: u64 = 2;

// Max number of seconds to wait for a disconnect confirmation
const DISCONNECT_WAIT_PERIOD: u64 = 5;

type RequestHandler = Arc<
    dyn Fn(Bytes) -> Pin<Box<dyn Future<Output = Result<Bytes, RequestError>> + Send>>
        + Send
        + Sync,
>;
type RequestHandlerRegistry = Arc<Mutex<HashMap<String, RequestHandler>>>;

#[derive(thiserror::Error, Debug)]
pub enum TLSSettingError {
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
pub enum RequestError {
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

    #[error("Invalid request format")]
    InvalidRequestFormat(#[from] serde_json::Error),

    #[error("Unexpected error")]
    Unexpected,
}

#[derive(thiserror::Error, Debug)]
pub enum InterfaceError {
    #[error("TLS config failed")]
    TLSConfigFailed(#[from] TLSSettingError),

    #[error("Couldn't disconnect cleanly")]
    DisconnectFailed(#[from] Box<ClientError>),

    #[error("Couldn't disconnect cleanly")]
    DisconnectTimeout(#[from] Elapsed),

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

pub struct Interface {
    client: AsyncClient,
    executor: Option<InterfaceExecutor>,
    handlers: RequestHandlerRegistry,
    token: CancellationToken,
}

impl Interface {
    pub fn new(
        client_settings: MQTTSettings,
        token: CancellationToken,
    ) -> Result<Self, InterfaceError> {
        let mut options = MqttOptions::new("", client_settings.host, client_settings.port);
        options.set_connection_timeout(client_settings.connection_timeout.0);

        if let Some(tls_settings) = client_settings.tls {
            set_tls_settings(&mut options, tls_settings)?;
        }

        let (client, eventloop) = AsyncClient::new(options.clone(), CLIENT_CHANNEL_CAPACITY);

        let handlers = Arc::new(Mutex::new(HashMap::new()));
        let executor = InterfaceExecutor {
            client: client.clone(),
            eventloop: Arc::new(Mutex::new(eventloop)),
            handlers: handlers.clone(),
            request_semaphore: Arc::new(Semaphore::new(REQUEST_CONCURRENCY)),
        };

        Ok(Interface {
            client,
            executor: Some(executor),
            handlers,
            token,
        })
    }

    pub async fn run(&mut self) -> Result<(), InterfaceError> {
        let mut executor = self.executor.take().ok_or(InterfaceError::AlreadyRunning)?;

        tokio::select! {
            _ = executor.poll() => {},
            _ = self.token.cancelled() => {}
        }

        self.client
            .disconnect()
            .await
            .map_err(|e| InterfaceError::DisconnectFailed(Box::new(e)))?;
        tokio::time::timeout(Duration::from_secs(DISCONNECT_WAIT_PERIOD), executor.poll()).await?;
        Ok(())
    }

    pub async fn add_handler<F, O, M, R, E>(&self, topic: &str, func: F) -> Result<(), ClientError>
    where
        M: for<'a> Deserialize<'a>,
        R: Serialize,
        E: Debug,
        O: Future<Output = Result<R, E>> + Send,
        F: Fn(M) -> O + Send + Sync + 'static,
    {
        let mut handlers = self.handlers.lock().await;
        let func_ref = Arc::new(func);
        handlers.insert(
            topic.into(),
            Arc::new(move |request| {
                let func = func_ref.clone();
                Box::pin(async move {
                    let message = serde_json::from_slice(&request)?;
                    let response = func(message)
                        .await
                        .inspect_err(|e| tracing::error!("Handler failed: {e:?}"))
                        .map_err(|_| RequestError::Unexpected)?;
                    let response = serde_json::to_vec(&response)?;
                    Ok(response.into())
                })
            }),
        );
        self.client.subscribe(topic, QoS::AtMostOnce).await?;
        Ok(())
    }

    // pub async fn publish(&self, topic: &str, data: Bytes) -> Result<(), ClientError> {
    //     self.client
    //         .publish_bytes(topic, QoS::AtMostOnce, false, data)
    //         .await
    // }
}

struct InterfaceExecutor {
    client: AsyncClient,
    eventloop: Arc<Mutex<EventLoop>>,
    handlers: RequestHandlerRegistry,
    request_semaphore: Arc<Semaphore>,
}

impl InterfaceExecutor {
    async fn poll(&mut self) {
        let mut eventloop = self.eventloop.lock().await;
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(notification) => {
                    tracing::debug!("Received = {notification:?}");
                    match notification {
                        Event::Incoming(Packet::ConnAck(packet)) => {
                            tracing::info!("Client connected: {:?}", packet.code);
                        }
                        Event::Incoming(Packet::Disconnect(packet)) => {
                            tracing::warn!("Client disconnected: {:?}", packet.reason_code);
                        }
                        Event::Outgoing(Outgoing::Disconnect) => {
                            tracing::info!("Disconnecting...");
                            break;
                        }
                        Event::Incoming(Packet::Publish(packet)) => {
                            if let Err(e) = self.accept(packet).await {
                                tracing::error!("Failed to process request: {e:?}");
                            };
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    tracing::error!("Error = {e:?}");
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY)).await;
                }
            }
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

        let client = self.client.clone();

        let handlers = self.handlers.lock().await;
        let Some(handler) = handlers.get(&topic) else {
            return Ok(());
        };

        let handler = handler.clone();

        tokio::spawn(async move {
            let response = match handler(packet.payload).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Request handler failed: {e:?}");
                    return;
                }
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
                tracing::error!("Failed to publish response: {e:?}");
            };
            drop(permit);
        });

        Ok(())
    }
}

// #[derive(Clone)]
// struct RequestHandler {
//     client: AsyncClient,
//     proxies: RequestProxyRegistry,
//     request_semaphore: Arc<Semaphore>,
// }

// impl RequestHandler {
//     fn new(client: AsyncClient, proxies: RequestProxyRegistry) -> Self {
//         RequestHandler {
//             client,
//             proxies,
//             request_semaphore: Arc::new(Semaphore::new(REQUEST_CONCURRENCY)),
//         }
//     }

// }
