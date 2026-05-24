//! Protobuf-encoded replicated commands (`StateCommand` from `state_command.proto`).

use prost::Message;

pub use crate::proto::state_command::{
    state_command, DeleteElementCommand, DeleteIfExpiredCommand, ExpireElementCommand,
    SetElementCommand, SetElementIfNotExistCommand, StartNewTermCommand, StateCommand,
};

/// Encode a [`StateCommand`] to its canonical protobuf wire format.
pub fn encode(cmd: &StateCommand) -> Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    cmd.encode(&mut buf)?;
    Ok(buf)
}

/// Decode a [`StateCommand`] from protobuf bytes.
pub fn decode(bytes: &[u8]) -> Result<StateCommand, prost::DecodeError> {
    StateCommand::decode(bytes)
}

impl StateCommand {
    pub fn set_element(name: impl Into<String>, value: Vec<u8>, expire_at: Option<u64>) -> Self {
        Self {
            op: Some(state_command::Op::SetElement(SetElementCommand {
                name: name.into(),
                value,
                expire_at,
            })),
        }
    }

    pub fn set_element_if_not_exist(
        name: impl Into<String>,
        value: Vec<u8>,
        expire_at: Option<u64>,
    ) -> Self {
        Self {
            op: Some(state_command::Op::SetElementIfNotExist(
                SetElementIfNotExistCommand {
                    name: name.into(),
                    value,
                    expire_at,
                },
            )),
        }
    }

    pub fn delete_element(name: impl Into<String>) -> Self {
        Self {
            op: Some(state_command::Op::DeleteElement(DeleteElementCommand {
                name: name.into(),
            })),
        }
    }

    pub fn expire_element(name: impl Into<String>, expire_at: u64) -> Self {
        Self {
            op: Some(state_command::Op::ExpireElement(ExpireElementCommand {
                name: name.into(),
                expire_at,
            })),
        }
    }

    pub fn delete_if_expired(name: impl Into<String>, now: u64) -> Self {
        Self {
            op: Some(state_command::Op::DeleteIfExpired(DeleteIfExpiredCommand {
                name: name.into(),
                now,
            })),
        }
    }

    pub fn start_new_term() -> Self {
        Self {
            op: Some(state_command::Op::StartNewTerm(StartNewTermCommand {})),
        }
    }
}
