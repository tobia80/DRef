package io.github.tobia80.dref.raft.proto

import com.google.protobuf.ByteString
import io.github.tobia80.dref_consensus.*
import io.github.tobia80.dref_consensus.ZioDrefConsensus.DRefConsensusClient
import io.github.tobia80.state_command.StateCommand
import scalapb.zio_grpc.ZManagedChannel
import zio.*
import zio.stream.ZStream

enum Role {
  case Follower, Candidate, Leader
}

private final case class ConsensusState(
  role: Role,
  term: Long,
  votedFor: Option[String],
  leaderId: Option[String],
  lastSeq: Long,
  lastHeartbeatNanos: Long
)

sealed trait ConsensusError extends Throwable
object ConsensusError {
  final case class NotLeader(leaderId: Option[String]) extends ConsensusError {
    override def getMessage: String = s"not leader; current leader id = $leaderId"
  }
  case object NoLeader extends ConsensusError {
    override def getMessage: String = "no leader is currently elected"
  }
  final case class Serialize(message: String) extends ConsensusError
  final case class Transport(message: String) extends ConsensusError
}

final class ProtoConsensusEngine private (
  val nodeId: String,
  val stateMachine: ProtoStateMachine,
  peers: Map[String, DRefConsensusClient],
  config: ProtoRaftConfig,
  stateRef: Ref[ConsensusState]
) {
  def role: UIO[Role] = stateRef.get.map(_.role)

  def isLeader: UIO[Boolean] = role.map(_ == Role.Leader)

  def leaderId: UIO[Option[String]] = stateRef.get.map(_.leaderId)

  def submit(cmd: StateCommand): IO[ConsensusError, ApplyResult] =
    for {
      termAndSeq <- stateRef.modify { st =>
                      if st.role != Role.Leader then (Left(ConsensusError.NotLeader(st.leaderId)), st)
                      else
                        val next = st.copy(lastSeq = st.lastSeq + 1)
                        (Right((next.term, next.lastSeq)), next)
                    }.flatMap {
                      case Left(err)  => ZIO.fail(err)
                      case Right(pair) => ZIO.succeed(pair)
                    }
      (term, seq) = termAndSeq
      bytes       = cmd.toByteArray
      result     <- stateMachine.apply(cmd)
      _          <- replicate(term, seq, bytes).forkDaemon.unit
    } yield result

  def handleAppendEntries(
    leaderId: String,
    term: Long,
    seq: Long,
    command: Array[Byte]
  ): UIO[(Boolean, Long)] =
    for {
      updated <- stateRef.modify { st =>
                   if term < st.term then ((false, st.term), st)
                   else
                     val stepped =
                       if term > st.term then st.copy(term = term, votedFor = None)
                       else st
                     val next = stepped.copy(
                       role = Role.Follower,
                       leaderId = Some(leaderId),
                       lastHeartbeatNanos = java.lang.System.nanoTime()
                     )
                     if seq != next.lastSeq + 1 then ((false, next.term), next)
                     else ((true, next.term), next.copy(lastSeq = seq))
                 }
      (accepted, currentTerm) = updated
      result                 <-
        if accepted then
          ZIO
            .attempt(StateCommand.parseFrom(command))
            .flatMap(stateMachine.apply)
            .fold(_ => false, _ => true)
            .map(ok => (ok, currentTerm))
        else ZIO.succeed((false, currentTerm))
    } yield result

  def handleHeartbeat(leaderId: String, term: Long): UIO[(Boolean, Long)] =
    stateRef.modify { st =>
      if term < st.term then ((false, st.term), st)
      else
        val stepped =
          if term > st.term then st.copy(term = term, votedFor = None)
          else st
        val next = stepped.copy(
          role = Role.Follower,
          leaderId = Some(leaderId),
          lastHeartbeatNanos = java.lang.System.nanoTime()
        )
        ((true, next.term), next)
    }

  def handleVote(candidateId: String, term: Long, lastSeq: Long): UIO[(Boolean, Long)] =
    stateRef.modify { st =>
      if term < st.term then ((false, st.term), st)
      else
        val stepped =
          if term > st.term then st.copy(term = term, votedFor = None, role = Role.Follower)
          else st
        val upToDate = lastSeq >= stepped.lastSeq
        val canVote  = stepped.votedFor.forall(_ == candidateId)
        val granted  = upToDate && canVote
        val next =
          if granted then
            stepped.copy(
              votedFor = Some(candidateId),
              lastHeartbeatNanos = java.lang.System.nanoTime()
            )
          else stepped
        ((granted, next.term), next)
    }

  def handleInstallSnapshot(
    leaderId: String,
    term: Long,
    snapshot: ClusterSnapshot,
    lastSeq: Long
  ): UIO[(Boolean, Long)] =
    for {
      updated <- stateRef.modify { st =>
                   if term < st.term then ((false, st.term), st)
                   else
                     val stepped =
                       if term > st.term then st.copy(term = term, votedFor = None)
                       else st
                     val next = stepped.copy(
                       role = Role.Follower,
                       leaderId = Some(leaderId),
                       lastHeartbeatNanos = java.lang.System.nanoTime(),
                       lastSeq = lastSeq
                     )
                     ((true, next.term), next)
                 }
      (accepted, currentTerm) = updated
      _                    <- stateMachine.installSnapshot(snapshot).when(accepted)
    } yield (accepted, currentTerm)

  def spawnDrivers: UIO[Fiber.Runtime[Throwable, Nothing]] =
    driverLoop.forever.forkDaemon

  private def replicate(term: Long, seq: Long, command: Array[Byte]): UIO[Unit] =
    ZIO.foreachDiscard(peers) { case (peerId, client) =>
      client
        .appendEntries(
          AppendEntriesRequest(
            leaderId = nodeId,
            term = term,
            command = ByteString.copyFrom(command),
            seq = seq
          )
        )
        .catchAll(_ => ZIO.unit)
    }

  private val driverLoop: UIO[Unit] =
    for {
      currentRole <- role
      _           <- currentRole match {
                       case Role.Leader              => sendHeartbeats
                       case Role.Follower | Role.Candidate =>
                         for {
                           now <- Clock.nanoTime
                           st  <- stateRef.get
                           jitter <- Random.nextLongBetween(
                                       0L,
                                       math.max(1L, config.electionTimeout.toMillis / 2)
                                     )
                           timeoutNanos = config.electionTimeout.toNanos + (jitter * 1_000_000L)
                           elapsed      = now - st.lastHeartbeatNanos
                           _           <- startElection.when(elapsed >= timeoutNanos)
                         } yield ()
                     }
      _ <- ZIO.sleep(config.heartbeatInterval)
    } yield ()

  private def sendHeartbeats: UIO[Unit] =
    stateRef.get.flatMap { st =>
      ZIO.foreachDiscard(peers) { case (peerId, client) =>
        client
          .heartbeat(HeartbeatRequest(leaderId = nodeId, term = st.term))
          .catchAll(_ => ZIO.unit)
      }
    }

  private def startElection: UIO[Unit] =
    for {
      election <- stateRef.modify { st =>
                    val next = st.copy(
                      role = Role.Candidate,
                      term = st.term + 1,
                      votedFor = Some(nodeId),
                      leaderId = None,
                      lastHeartbeatNanos = java.lang.System.nanoTime()
                    )
                    ((next.term, next.lastSeq), next)
                  }
      (term: Long, lastSeq: Long) = election
      _              <- ZIO.logInfo(s"starting election on node $nodeId term $term")
      responses      <- ZIO.foreachPar(peers.toList) { case (peerId, client) =>
                          client
                            .requestVote(VoteRequest(candidateId = nodeId, term = term, lastSeq = lastSeq))
                            .map(Some(_))
                            .catchAll { _ =>
                              ZIO.logDebug(s"vote request failed for peer $peerId") *> ZIO.none
                            }
                        }
      higherTerm = responses.flatten.collect { case resp if resp.term > term => resp.term }.headOption
      _         <- higherTerm match {
                     case Some(newTerm) =>
                       stateRef.update { st =>
                         if newTerm > st.term then st.copy(term = newTerm, role = Role.Follower, votedFor = None)
                         else st
                       }
                     case None          =>
                       val votes = 1 + responses.flatten.count(_.granted)
                       val needed = (peers.size + 1) / 2 + 1
                       if votes >= needed then
                         stateRef.modify { st =>
                           if st.role == Role.Candidate && st.term == term then
                             val next = st.copy(role = Role.Leader, leaderId = Some(nodeId))
                             (true, next)
                           else (false, st)
                         }.flatMap { elected =>
                           ZIO.logInfo(s"node $nodeId elected leader term $term").when(elected) *>
                             broadcastSnapshot.when(elected)
                         }
                       else ZIO.logInfo(s"node $nodeId lost election term $term votes $votes")
                   }
    } yield ()

  private def broadcastSnapshot: UIO[Unit] =
    for {
      st       <- stateRef.get
      snapshot <- stateMachine.takeSnapshot
      _        <- ZIO.foreachDiscard(peers) { case (peerId, client) =>
                    client
                      .installSnapshot(
                        InstallSnapshotRequest(
                          leaderId = nodeId,
                          term = st.term,
                          snapshot = Some(snapshot),
                          lastSeq = st.lastSeq
                        )
                      )
                      .catchAll(_ => ZIO.unit)
                  }
    } yield ()
}

object ProtoConsensusEngine {
  def make(
    nodeId: String,
    stateMachine: ProtoStateMachine,
    config: ProtoRaftConfig
  ): ZIO[Scope, Throwable, ProtoConsensusEngine] =
    for {
      peerEntries <- ZIO.foreach(config.initialEndpoints.filter(_.id != nodeId)) { ep =>
                       DRefConsensusClient
                         .scoped(GrpcChannels.managedChannel(ep.address))
                         .map(ep.id -> _)
                     }
      peers          = peerEntries.toMap
      initialRole    = if peers.isEmpty then Role.Leader else Role.Follower
      initialLeader  = if initialRole == Role.Leader then Some(nodeId) else None
      initialTerm    = if initialRole == Role.Leader then 1L else 0L
      stateRef      <- Ref.make(
                         ConsensusState(
                           role = initialRole,
                           term = initialTerm,
                           votedFor = None,
                           leaderId = initialLeader,
                           lastSeq = 0L,
                           lastHeartbeatNanos = java.lang.System.nanoTime()
                         )
                       )
    } yield new ProtoConsensusEngine(nodeId, stateMachine, peers, config, stateRef)

  def waitForLeader(engine: ProtoConsensusEngine, max: Duration): UIO[Option[String]] =
    ZStream
      .repeatZIOWithSchedule(engine.leaderId, Schedule.spaced(50.millis) && Schedule.upTo(max))
      .runCollect
      .map(_.collectFirst { case Some(id) => id })
}
