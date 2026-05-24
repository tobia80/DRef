//! gRPC server implementations for both:
//!
//! 1. The public **DRefRaft** service (from `dref.proto`) — bit-compatible
//!    with the Scala definition. Clients (other nodes, or end-users via
//!    [`crate::context::RaftDRefContext`]) send writes here; non-leader
//!    nodes return a `FAILED_PRECONDITION` with a `not-leader` description
//!    that the client uses to re-target the leader.
//!
//! 2. The internal **RaftInternal** service (from `raft_network.proto`) —
//!    used between nodes for AppendEntries, Heartbeat, RequestVote, and
//!    InstallSnapshot.
//!
//! Both services run on the same gRPC server (one port per node), matching
//! the Scala implementation which also hosts everything on a single
//! `ServerBuilder`.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::command::DRefCommand;
use crate::consensus::{Consensus, ConsensusError};
use crate::proto::dref::d_ref_raft_server::DRefRaft;
use crate::proto::dref::{
    DeleteElementRequest, DeleteElementResponse, EndpointResponse, ExpireElementRequest,
    ExpireElementResponse, GetElementRequest, GetElementResponse, GetEndpointsRequest,
    SendCommandRequest, SendCommandResponse, SetElementIfNotExistRequest,
    SetElementIfNotExistResponse, SetElementRequest, SetElementResponse,
};
use crate::proto::raft_network::raft_internal_server::RaftInternal;
use crate::proto::raft_network::{
    AppendEntriesRequest, AppendEntriesResponse, HeartbeatRequest, HeartbeatResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use crate::state_machine::{unix_millis, ApplyResult};

/// gRPC adapter for the public DRefRaft service. All requests are routed
/// through `consensus`; non-leaders surface `NotLeader` to the client as
/// described above.
#[derive(Clone)]
pub struct DRefRaftService {
    consensus: Arc<Consensus>,
    /// Known node ids, returned by `GetEndpoints`. The list matches the
    /// fixed cluster membership; we don't currently support dynamic
    /// reconfiguration.
    endpoint_ids: Vec<String>,
}

impl DRefRaftService {
    pub fn new(consensus: Arc<Consensus>, endpoint_ids: Vec<String>) -> Self {
        Self {
            consensus,
            endpoint_ids,
        }
    }

    /// Translate a consensus error into a gRPC `Status`. We use
    /// `FAILED_PRECONDITION` with a description starting with `not-leader`
    /// so the client can recognise it without depending on metadata layout.
    fn map_err(e: ConsensusError) -> Status {
        match e {
            ConsensusError::NotLeader { leader_id } => {
                let desc = format!(
                    "not-leader:{}",
                    leader_id.unwrap_or_else(|| "unknown".to_string())
                );
                Status::failed_precondition(desc)
            }
            ConsensusError::NoLeader => {
                Status::failed_precondition("no-leader:unknown")
            }
            ConsensusError::Serialize(s) => Status::internal(format!("serialize: {s}")),
            ConsensusError::Transport(s) => Status::unavailable(s),
        }
    }
}

#[tonic::async_trait]
impl DRefRaft for DRefRaftService {
    async fn set_element(
        &self,
        request: Request<SetElementRequest>,
    ) -> Result<Response<SetElementResponse>, Status> {
        let r = request.into_inner();
        let cmd = DRefCommand::SetElement {
            name: r.name,
            value: r.value,
            expire_at: r.expire_at,
        };
        self.consensus
            .submit(cmd)
            .await
            .map_err(Self::map_err)?;
        Ok(Response::new(SetElementResponse {}))
    }

    async fn set_element_if_not_exist(
        &self,
        request: Request<SetElementIfNotExistRequest>,
    ) -> Result<Response<SetElementIfNotExistResponse>, Status> {
        let r = request.into_inner();
        let cmd = DRefCommand::SetElementIfNotExist {
            name: r.name,
            value: r.value,
            expire_at: r.expire_at,
        };
        let res = self.consensus.submit(cmd).await.map_err(Self::map_err)?;
        let created = match res {
            ApplyResult::Created(c) => c,
            // Any other variant from this command would be a bug; default
            // to "not created" so we never falsely claim a write happened.
            _ => false,
        };
        Ok(Response::new(SetElementIfNotExistResponse { created }))
    }

    async fn get_element(
        &self,
        request: Request<GetElementRequest>,
    ) -> Result<Response<GetElementResponse>, Status> {
        let r = request.into_inner();
        // Reads go straight to the local state machine. We require the
        // request to be served by the leader to match Scala's
        // `QueryPolicy.LINEARIZABLE`. Non-leaders bounce to the leader.
        if !self.consensus.is_leader().await {
            return Err(Self::map_err(ConsensusError::NotLeader {
                leader_id: self.consensus.leader_id().await,
            }));
        }
        let value = self.consensus.state_machine.get(&r.name).await;
        Ok(Response::new(GetElementResponse { value }))
    }

    async fn delete_element(
        &self,
        request: Request<DeleteElementRequest>,
    ) -> Result<Response<DeleteElementResponse>, Status> {
        let r = request.into_inner();
        let cmd = DRefCommand::DeleteElement { name: r.name };
        self.consensus.submit(cmd).await.map_err(Self::map_err)?;
        Ok(Response::new(DeleteElementResponse {}))
    }

    async fn expire_element(
        &self,
        request: Request<ExpireElementRequest>,
    ) -> Result<Response<ExpireElementResponse>, Status> {
        let r = request.into_inner();
        let cmd = DRefCommand::ExpireElement {
            name: r.name,
            expire_at: r.expire_at,
        };
        self.consensus.submit(cmd).await.map_err(Self::map_err)?;
        Ok(Response::new(ExpireElementResponse {}))
    }

    async fn get_endpoints(
        &self,
        _request: Request<GetEndpointsRequest>,
    ) -> Result<Response<EndpointResponse>, Status> {
        Ok(Response::new(EndpointResponse {
            ids: self.endpoint_ids.clone(),
        }))
    }

    async fn send_command(
        &self,
        _request: Request<SendCommandRequest>,
    ) -> Result<Response<SendCommandResponse>, Status> {
        // The Scala impl uses `SendCommand` as the transport for
        // MicroRaft messages. Our Rust port carries Raft messages on the
        // separate `RaftInternal` service, so this RPC is a no-op
        // accepted for wire compatibility. A future iteration can route
        // the bytes into the consensus layer.
        let _ = unix_millis(); // touch to keep import used
        Ok(Response::new(SendCommandResponse {}))
    }
}

/// gRPC adapter for the internal Raft service. Pure forwarder onto
/// [`Consensus`] handlers.
#[derive(Clone)]
pub struct RaftInternalService {
    consensus: Arc<Consensus>,
}

impl RaftInternalService {
    pub fn new(consensus: Arc<Consensus>) -> Self {
        Self { consensus }
    }
}

#[tonic::async_trait]
impl RaftInternal for RaftInternalService {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let r = request.into_inner();
        let (success, term) = self
            .consensus
            .handle_append_entries(r.leader_id, r.term, r.seq, r.command)
            .await;
        Ok(Response::new(AppendEntriesResponse { success, term }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let r = request.into_inner();
        let (acknowledged, term) = self.consensus.handle_heartbeat(r.leader_id, r.term).await;
        Ok(Response::new(HeartbeatResponse {
            acknowledged,
            term,
        }))
    }

    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let r = request.into_inner();
        let (granted, term) = self
            .consensus
            .handle_vote(r.candidate_id, r.term, r.last_seq)
            .await;
        Ok(Response::new(VoteResponse { granted, term }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let r = request.into_inner();
        let (success, term) = self
            .consensus
            .handle_install_snapshot(r.leader_id, r.term, r.snapshot, r.last_seq)
            .await;
        Ok(Response::new(InstallSnapshotResponse { success, term }))
    }
}
