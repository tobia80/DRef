//! gRPC server implementations for both:
//!
//! 1. The public **DRefRaft** service (from `dref.proto`) — bit-compatible
//!    with the Scala definition.
//! 2. The internal **DRefConsensus** service (from `dref_consensus.proto`) —
//!    used between nodes for cross-language replication.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::consensus::{Consensus, ConsensusError};
use crate::proto::dref::d_ref_raft_server::DRefRaft;
use crate::proto::dref::{
    DeleteElementRequest, DeleteElementResponse, EndpointResponse, ExpireElementRequest,
    ExpireElementResponse, GetElementRequest, GetElementResponse, GetEndpointsRequest,
    SendCommandRequest, SendCommandResponse, SetElementIfNotExistRequest,
    SetElementIfNotExistResponse, SetElementRequest, SetElementResponse,
};
use crate::proto::dref_consensus::d_ref_consensus_server::DRefConsensus;
use crate::proto::dref_consensus::{
    AppendEntriesRequest, AppendEntriesResponse, HeartbeatRequest, HeartbeatResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, PreVoteRequest, PreVoteResponse, VoteRequest,
    VoteResponse,
};
use crate::state_command::StateCommand;
use crate::state_machine::{unix_millis, ApplyResult};

#[derive(Clone)]
pub struct DRefRaftService {
    consensus: Arc<Consensus>,
    endpoint_ids: Vec<String>,
}

impl DRefRaftService {
    pub fn new(consensus: Arc<Consensus>, endpoint_ids: Vec<String>) -> Self {
        Self {
            consensus,
            endpoint_ids,
        }
    }

    fn map_err(e: ConsensusError) -> Status {
        match e {
            ConsensusError::NotLeader { leader_id } => {
                let desc = format!(
                    "not-leader:{}",
                    leader_id.unwrap_or_else(|| "unknown".to_string())
                );
                Status::failed_precondition(desc)
            }
            ConsensusError::NoLeader => Status::failed_precondition("no-leader:unknown"),
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
        let cmd = StateCommand::set_element(r.name, r.value, r.expire_at);
        self.consensus.submit(cmd).await.map_err(Self::map_err)?;
        Ok(Response::new(SetElementResponse {}))
    }

    async fn set_element_if_not_exist(
        &self,
        request: Request<SetElementIfNotExistRequest>,
    ) -> Result<Response<SetElementIfNotExistResponse>, Status> {
        let r = request.into_inner();
        let cmd = StateCommand::set_element_if_not_exist(r.name, r.value, r.expire_at);
        let res = self.consensus.submit(cmd).await.map_err(Self::map_err)?;
        let created = match res {
            ApplyResult::Created(c) => c,
            _ => false,
        };
        Ok(Response::new(SetElementIfNotExistResponse { created }))
    }

    async fn get_element(
        &self,
        request: Request<GetElementRequest>,
    ) -> Result<Response<GetElementResponse>, Status> {
        let r = request.into_inner();
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
        let cmd = StateCommand::delete_element(r.name);
        self.consensus.submit(cmd).await.map_err(Self::map_err)?;
        Ok(Response::new(DeleteElementResponse {}))
    }

    async fn expire_element(
        &self,
        request: Request<ExpireElementRequest>,
    ) -> Result<Response<ExpireElementResponse>, Status> {
        let r = request.into_inner();
        let cmd = StateCommand::expire_element(r.name, r.expire_at);
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
        let _ = unix_millis();
        Ok(Response::new(SendCommandResponse {}))
    }
}

#[derive(Clone)]
pub struct DRefConsensusService {
    consensus: Arc<Consensus>,
}

impl DRefConsensusService {
    pub fn new(consensus: Arc<Consensus>) -> Self {
        Self { consensus }
    }
}

#[tonic::async_trait]
impl DRefConsensus for DRefConsensusService {
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
        Ok(Response::new(HeartbeatResponse { acknowledged, term }))
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

    async fn request_pre_vote(
        &self,
        request: Request<PreVoteRequest>,
    ) -> Result<Response<PreVoteResponse>, Status> {
        let r = request.into_inner();
        let (granted, term) = self
            .consensus
            .handle_pre_vote(r.candidate_id, r.term, r.last_seq)
            .await;
        Ok(Response::new(PreVoteResponse { granted, term }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let r = request.into_inner();
        let snapshot = r.snapshot.unwrap_or_default();
        let (success, term) = self
            .consensus
            .handle_install_snapshot(r.leader_id, r.term, snapshot, r.last_seq)
            .await;
        Ok(Response::new(InstallSnapshotResponse { success, term }))
    }
}
