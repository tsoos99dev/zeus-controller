use std::{
    collections::HashMap, future::Future, pin::Pin, string::FromUtf8Error, sync::Arc,
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
use tokio::sync::{Mutex, Semaphore};
use tokio_rustls::rustls::pki_types;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    app_config::{MQTTSettings, TLSSettings},
    ops::AsyncFn,
    service::{Proxy, ServiceError},
};

// Number of messages allowed in the async client's buffer
const CLIENT_CHANNEL_CAPACITY: usize = 20;

// Amount of time to wait before advancing the loop after an error
const RETRY_DELAY: u64 = 1;

// Max number of requests allowed to be processed concurrently
const REQUEST_CONCURRENCY: usize = 5;

// Max number of seconds to wait for an available handler before the packet is dropped
const REQUEST_WAIT_PERIOD: u64 = 2;

#[typetag::deserialize]
pub trait Request {}

#[typetag::serialize]
pub trait Response {}

type RequestProxyRegistry = Arc<
    Mutex<
        HashMap<
            String,
            Box<dyn AsyncFn<Args = Bytes, Ret = Result<Bytes, RequestError>> + Send + Sync>,
        >,
    >,
>;

// impl<T, F> AsyncFnOnce for T
// where
//     T: FnOnce(&Bytes) -> F,
//     F: Future<Output = Bytes> + Send + Sync + 'static,
// {
//     type Args = Bytes;
//     type Ret = Bytes;
//     fn call(self, args: Bytes) -> Pin<Box<dyn Future<Output = Bytes> + Send + Sync>> {
//         Box::pin(self(&args))
//     }
// }

impl<T, F> AsyncFn for T
where
    T: Fn(Bytes) -> F,
    F: Future<Output = Result<Bytes, RequestError>> + Send + Sync + 'static,
{
    type Args = Bytes;
    type Ret = Result<Bytes, RequestError>;
    fn call(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = Self::Ret> + Send + Sync>> {
        Box::pin(self(args))
    }
}

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

    #[error("Handler failed")]
    HandlerFailed(#[from] ServiceError),
}

#[derive(thiserror::Error, Debug)]
pub enum InterfaceError {
    #[error("TLS config failed")]
    TLSConfigFailed(#[from] TLSSettingError),

    #[error("Couldn't disconnect cleanly")]
    DisconnectFailed(#[from] Box<ClientError>),

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
    proxies: RequestProxyRegistry,
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

        let proxies = Arc::new(Mutex::new(HashMap::new()));
        let executor = InterfaceExecutor {
            eventloop,
            request_handler: RequestHandler::new(client.clone(), proxies.clone()),
        };

        Ok(Interface {
            client,
            executor: Some(executor),
            proxies,
            token,
        })
    }

    pub async fn run(&mut self) -> Result<(), InterfaceError> {
        let tracker = TaskTracker::new();

        // let internal_cancellation_token_clone = self.internal_cancellation_token.clone();
        let mut executor = self.executor.take().ok_or(InterfaceError::AlreadyRunning)?;

        let token = CancellationToken::new();
        let token_clone = token.clone();

        tracker.spawn(async move {
            tokio::select! {
                _ = executor.poll() => {},
                _ = token_clone.cancelled() => {}
            }
        });

        self.token.cancelled().await;

        tracker.close();

        let state = self
            .client
            .disconnect()
            .await
            .map_err(|e| InterfaceError::DisconnectFailed(Box::new(e)));

        token.cancel();
        tracker.wait().await;
        state
    }

    pub async fn add_handler<M, R>(
        &self,
        topic: &str,
        proxy: Proxy<M, R>,
    ) -> Result<(), ClientError>
    where
        M: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
        R: Serialize + Clone + Send + Sync + 'static,
    {
        let mut proxies = self.proxies.lock().await;
        let converter = Box::new(move |request: Bytes| {
            let proxy = proxy.clone();
            async move {
                let message = serde_json::from_slice(&request)?;
                let response = proxy.send(message).await?;
                let response = serde_json::to_vec(&response)?;
                Ok(response.into())
            }
        });

        proxies.insert(topic.into(), converter);
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
    eventloop: EventLoop,
    request_handler: RequestHandler,
}

impl InterfaceExecutor {
    async fn poll(&mut self) {
        loop {
            let event = self.eventloop.poll().await;
            match event {
                Ok(notification) => {
                    println!("Received = {notification:?}");
                    match notification {
                        Event::Incoming(Packet::ConnAck(packet)) => {
                            println!("Client connected: {:?}", packet.code);
                        }
                        Event::Incoming(Packet::Disconnect(packet)) => {
                            println!("Client disconnected: {:?}", packet.reason_code);
                        }
                        Event::Outgoing(Outgoing::Disconnect) => {
                            println!("Disconnecting...");
                        }
                        Event::Incoming(Packet::Publish(packet)) => {
                            if let Err(e) = self.request_handler.accept(packet).await {
                                eprintln!("Failed to process request: {e:?}");
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
    proxies: RequestProxyRegistry,
    request_semaphore: Arc<Semaphore>,
}

impl RequestHandler {
    fn new(client: AsyncClient, proxies: RequestProxyRegistry) -> Self {
        RequestHandler {
            client,
            proxies,
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

        let proxies = self.proxies.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let proxies = proxies.lock().await;
            let Some(proxy) = proxies.get(&topic) else {
                return;
            };

            let response = match proxy.call(packet.payload).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Request handler failed: {e:?}");
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
                eprintln!("Failed to publish response: {e:?}");
            };
            drop(permit);
        });

        Ok(())
    }
}
