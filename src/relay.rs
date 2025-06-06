use std::{io, time::Duration};

use client::Context;
use itertools::Itertools;
use serde::{self, Deserialize, Serialize};
use tokio_serial::SerialStream;

use tokio_modbus::prelude::*;

use crate::service::Service;

#[derive(thiserror::Error, Debug)]
#[error("IO Error")]
pub struct IOError(io::Error);

impl Serialize for IOError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}", self.0))
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Modbus error")]
pub struct TokioModbusError(String);

impl Serialize for TokioModbusError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{self}"))
    }
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct TokioModbusExceptionCode(tokio_modbus::ExceptionCode);

impl Serialize for TokioModbusExceptionCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{self}"))
    }
}

#[derive(thiserror::Error, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestFailureKind {
    #[error("Request failed")]
    Connection(#[source] TokioModbusError),

    #[error("Request failed")]
    Modbus(#[source] TokioModbusExceptionCode),
}

#[derive(thiserror::Error, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceError {
    #[error("Failed to connect to relay")]
    FailedToConnect(#[source] IOError),

    #[error("Timeout")]
    Timeout,

    #[error(transparent)]
    RequestFailed(#[from] RequestFailureKind),

    #[error("Failed to disconnect")]
    FailedToDisconnect(#[source] IOError),

    #[error("Invalid response")]
    InvalidResponse,
}

#[derive(Serialize)]
pub struct State([bool; 4]);

#[derive(Deserialize)]
pub enum Command {
    #[serde(alias = "set")]
    SetOutput { number: u16, value: bool },

    #[serde(alias = "status")]
    ReadStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    Success,
    Failure(InterfaceError),

    #[serde(untagged)]
    Status(State),
}

#[derive(Debug)]
pub struct Interface {
    device: String,
    unit_id: u8,
    baud_rate: u32,
    timeout: Duration,
}

impl Interface {
    pub fn new(device: &str, unit_id: u8, baud_rate: u32, timeout: Duration) -> Self {
        Interface {
            device: String::from(device),
            unit_id,
            baud_rate,
            timeout,
        }
    }

    pub async fn read_status(&self) -> Result<State, InterfaceError> {
        let mut ctx = self
            .create_ctx()
            .map_err(|e| InterfaceError::FailedToConnect(IOError(e)))?;

        let state = tokio::time::timeout(self.timeout, ctx.read_coils(0x0, 8))
            .await
            .map_err(|_| InterfaceError::Timeout)?
            .map_err(|e| RequestFailureKind::Connection(TokioModbusError(e.to_string())))?
            .map_err(|e| RequestFailureKind::Modbus(TokioModbusExceptionCode(e)))?;

        let Some(state) = state.iter().cloned().take(4).collect_array() else {
            return Err(InterfaceError::InvalidResponse);
        };
        ctx.disconnect()
            .await
            .map_err(|e| InterfaceError::FailedToDisconnect(IOError(e)))?;

        Ok(State(state))
    }

    pub async fn set_output(&self, number: u16, value: bool) -> Result<(), InterfaceError> {
        let mut ctx = self
            .create_ctx()
            .map_err(|e| InterfaceError::FailedToConnect(IOError(e)))?;

        tokio::time::timeout(self.timeout, ctx.write_single_coil(number, value))
            .await
            .map_err(|_| InterfaceError::Timeout)?
            .map_err(|e| RequestFailureKind::Connection(TokioModbusError(e.to_string())))?
            .map_err(|e| RequestFailureKind::Modbus(TokioModbusExceptionCode(e)))?;

        ctx.disconnect()
            .await
            .map_err(|e| InterfaceError::FailedToDisconnect(IOError(e)))?;

        Ok(())
    }

    fn create_ctx(&self) -> Result<Context, io::Error> {
        let slave = Slave(self.unit_id);
        let builder = tokio_serial::new(&self.device, self.baud_rate);
        let port = SerialStream::open(&builder)?;
        let ctx = rtu::attach_slave(port, slave);
        Ok(ctx)
    }
}

impl Service for Interface {
    type Message = Command;
    type Response = Response;

    async fn receive(&self, message: Self::Message) -> Self::Response {
        match message {
            Command::SetOutput { number, value } => {
                let result = self.set_output(number, value).await;
                match result {
                    Ok(_) => Response::Success,
                    Err(e) => Response::Failure(e),
                }
            }
            Command::ReadStatus => {
                let result = self.read_status().await;
                match result {
                    Ok(state) => Response::Status(state),
                    Err(e) => Response::Failure(e),
                }
            }
        }
    }
}
