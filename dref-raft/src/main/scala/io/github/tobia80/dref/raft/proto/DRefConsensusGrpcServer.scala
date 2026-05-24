package io.github.tobia80.dref.raft.proto

import io.github.tobia80.dref_consensus.*
import io.github.tobia80.dref_consensus.ZioDrefConsensus.ZDRefConsensus
import io.grpc.{Status, StatusException}
import scalapb.zio_grpc.RequestContext
import zio.{IO, ZIO}

final class DRefConsensusGrpcServer(consensus: ProtoConsensusEngine) extends ZDRefConsensus[RequestContext] {

  override def appendEntries(
    request: AppendEntriesRequest,
    context: RequestContext
  ): IO[StatusException, AppendEntriesResponse] =
    consensus
      .handleAppendEntries(
        request.leaderId,
        request.term,
        request.seq,
        request.command.toByteArray
      )
      .map { case (success, term) => AppendEntriesResponse(success, term) }

  override def heartbeat(
    request: HeartbeatRequest,
    context: RequestContext
  ): IO[StatusException, HeartbeatResponse] =
    consensus
      .handleHeartbeat(request.leaderId, request.term)
      .map { case (acknowledged, term) => HeartbeatResponse(acknowledged, term) }

  override def requestVote(
    request: VoteRequest,
    context: RequestContext
  ): IO[StatusException, VoteResponse] =
    consensus
      .handleVote(request.candidateId, request.term, request.lastSeq)
      .map { case (granted, term) => VoteResponse(granted, term) }

  override def installSnapshot(
    request: InstallSnapshotRequest,
    context: RequestContext
  ): IO[StatusException, InstallSnapshotResponse] =
    consensus
      .handleInstallSnapshot(
        request.leaderId,
        request.term,
        request.snapshot.getOrElse(ClusterSnapshot()),
        request.lastSeq
      )
      .map { case (success, term) => InstallSnapshotResponse(success, term) }
}
