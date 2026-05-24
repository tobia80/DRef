//! Rust port of `dref-raft`: a Raft-replicated `DRefContext` backend.
//!
//! The Scala original uses [MicroRaft](https://github.com/MicroRaft/MicroRaft)
//! for the consensus protocol and a gRPC service for inter-node transport.
//! For the Rust port we keep the public gRPC contract (`DRefRaft` service in
//! `dref.proto`) bit-compatible with Scala — so a Rust client could in
//! principle talk to a Scala server — but the inter-node consensus messages
//! use a Rust-only `RaftInternal` service defined in `raft_network.proto`
//! (the Scala impl tunnels Java-serialized `RaftMessage` objects, which
//! doesn't translate).
//!
//! The consensus engine itself is a deliberately simple leader/follower
//! protocol implemented in [`consensus`] — sufficient to provide
//! linearizable writes through a single elected leader, replicate state via
//! gRPC, and survive leader loss with re-election. It is NOT a production
//! Raft implementation (no persistent log, simplified election); it is the
//! "simple in-memory consensus wrapper" called out as acceptable in the
//! task spec.

pub mod command;
pub mod config;
pub mod consensus;
pub mod context;
pub mod grpc_client;
pub mod grpc_server;
pub mod state_machine;

/// Generated protobuf modules. Re-exported under [`proto`] so callers don't
/// need to know the package path Tonic puts them in.
pub mod proto {
    pub mod dref {
        tonic::include_proto!("io.github.tobia80");
    }

    pub mod raft_network {
        tonic::include_proto!("io.github.tobia80.rust.raft_network");
    }
}

pub use command::DRefCommand;
pub use config::{NodeEndpoint, RaftConfig};
pub use context::RaftDRefContext;
pub use state_machine::ExpiringValue;
