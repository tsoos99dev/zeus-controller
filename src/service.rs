use std::future::Future;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const MESSAGEBOX_SIZE: usize = 16;

#[derive(thiserror::Error, Debug, Serialize)]
pub enum ServiceError {
    #[error("Receiver gone")]
    ReceiverGone,

    #[error("Sender gone")]
    SenderGone,
}

pub struct Proxy<M, R> {
    sender: mpsc::Sender<(M, oneshot::Sender<R>)>,
    token: CancellationToken,
}

impl<M, R> Clone for Proxy<M, R> {
    fn clone(&self) -> Self {
        Proxy {
            sender: self.sender.clone(),
            token: self.token.clone(),
        }
    }
}

impl<M, R> Proxy<M, R> {
    pub async fn send(&self, message: M) -> Result<R, ServiceError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send((message, resp_tx))
            .await
            .map_err(|_| ServiceError::ReceiverGone)?;
        let response = resp_rx.await.map_err(|_| ServiceError::SenderGone)?;
        Ok(response)
    }

    // pub fn stop(self) {
    //     self.token.cancel();
    // }
}

pub trait Service
where
    Self: Sized + Send + Sync + 'static,
{
    type Message: Send + Sync;
    type Response: Send + Sync;

    fn spawn(self) -> Proxy<Self::Message, Self::Response> {
        let token = CancellationToken::new();
        let (sender, receiver) = mpsc::channel(MESSAGEBOX_SIZE);
        let proxy = Proxy {
            sender,
            token: token.clone(),
        };
        tokio::spawn(execute_service(receiver, self, token));
        proxy
    }

    fn receive(&self, message: Self::Message) -> impl Future<Output = Self::Response> + Send;

    fn stop(self) {}
}

async fn execute_service<S, M, R>(
    mut receiver: mpsc::Receiver<(M, oneshot::Sender<R>)>,
    service: S,
    token: CancellationToken,
) where
    S: Service<Message = M, Response = R>,
{
    tokio::select! {
        _ = async {
            while let Some((message, resp_channel)) = receiver.recv().await {
                let response = service.receive(message).await;
                let _ = resp_channel.send(response);
            }
        } => {},
        _ = token.cancelled() => {}
    }

    service.stop();
}
