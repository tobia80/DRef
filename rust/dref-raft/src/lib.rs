//! Rust port of `dref-raft`: a Raft-replicated `DRefContext` backend.
//!
//! Inter-node replication uses the shared protobuf `DRefConsensus` service
//! (`proto/dref_consensus.proto`) and protobuf `StateCommand` log entries
//! (`proto/state_command.proto`) so Scala and Rust nodes can participate in
//! the same cluster.

pub mod config;
pub mod consensus;
pub mod context;
pub mod grpc_client;
pub mod grpc_server;
pub mod state_command;
pub mod state_machine;

/// Generated protobuf modules.
pub mod proto {
    pub mod dref {
        tonic::include_proto!("io.github.tobia80");
    }

    pub mod raft {
        tonic::include_proto!("io.github.tobia80");
    }

    pub mod state_command {
        tonic::include_proto!("io.github.tobia80");
    }

    pub mod dref_consensus {
        tonic::include_proto!("io.github.tobia80");
    }
}

pub use config::{NodeEndpoint, RaftConfig};
pub use context::RaftDRefContext;
pub use state_command::StateCommand;
pub use state_machine::ExpiringValue;
