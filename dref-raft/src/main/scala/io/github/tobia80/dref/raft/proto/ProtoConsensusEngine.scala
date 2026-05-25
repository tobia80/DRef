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

final private case class ConsensusState(
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
  peersRef: Ref[Map[String, DRefConsensusClient]],
  config: ProtoRaftConfig,
  stateRef: Ref[ConsensusState],
  voterStore: VoterStateStore,
  persistMutex: Semaphore
) {

  /** Align consensus replication peers with the current discovery snapshot. */
  def syncPeers(
    endpoints: List[NodeEndpoint],
    selfNodeId: String,
    bindAddress: String
  ): ZIO[Scope, Throwable, Unit] =
    val desired = endpoints.filter(ep => ep.id != selfNodeId && ep.address != bindAddress)
    for {
      current <- peersRef.get
      toRemove = current.keySet -- desired.map(_.id).toSet
      toAdd    = desired.filter(ep => !current.contains(ep.id))
      _       <- ZIO.foreachDiscard(toRemove)(id => peersRef.update(_ - id))
      added   <- ZIO.foreach(toAdd) { ep =>
                   DRefConsensusClient
                     .scoped(GrpcChannels.managedChannel(ep.address))
                     .map(ep.id -> _)
                 }
      _       <- peersRef.update(_ ++ added.toMap).when(added.nonEmpty)
    } yield ()

  /** Run a state mutation and, if it changed `term` or `votedFor`, fsync the new voter state to disk *before* the
    * caller observes the result.
    *
    * Raft safety requires that a node never grants a vote or steps up to a higher term unless that decision is durable:
    * otherwise a crash-restart loop can produce two leaders in one term. The semaphore serializes persist calls so the
    * on-disk record matches the in-memory order of mutations even under concurrent RPCs.
    *
    * If the configured store throws (disk full, IO error) we `orDie` — a Raft node that can't durably record its vote
    * has no safe forward path and the process is expected to crash so the orchestrator restarts it.
    */
  private def updateAndPersist[A](f: ConsensusState => (A, ConsensusState)): UIO[A] =
    persistMutex.withPermit {
      for {
        before <- stateRef.get
        result <- stateRef.modify(f)
        after  <- stateRef.get
        _      <- ZIO
                    .when(after.term != before.term || after.votedFor != before.votedFor) {
                      voterStore.save(VoterState(after.term, after.votedFor)).orDie
                    }
      } yield result
    }
  def role: UIO[Role] = stateRef.get.map(_.role)

  def isLeader: UIO[Boolean] = role.map(_ == Role.Leader)

  def leaderId: UIO[Option[String]] = stateRef.get.map(_.leaderId)

  def submit(cmd: StateCommand): IO[ConsensusError, ApplyResult] =
    for {
      termAndSeq <- stateRef
                      .modify { st =>
                        if st.role != Role.Leader then (Left(ConsensusError.NotLeader(st.leaderId)), st)
                        else
                          val next = st.copy(lastSeq = st.lastSeq + 1)
                          (Right((next.term, next.lastSeq)), next)
                      }
                      .flatMap {
                        case Left(err)   => ZIO.fail(err)
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
      updated                <- updateAndPersist { st =>
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
    updateAndPersist { st =>
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
    updateAndPersist { st =>
      if term < st.term then ((false, st.term), st)
      else
        val stepped =
          if term > st.term then st.copy(term = term, votedFor = None, role = Role.Follower)
          else st
        val upToDate = lastSeq >= stepped.lastSeq
        val canVote = stepped.votedFor.forall(_ == candidateId)
        val granted = upToDate && canVote
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
      updated                <- updateAndPersist { st =>
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
      _                      <- stateMachine.installSnapshot(snapshot).when(accepted)
    } yield (accepted, currentTerm)

  def spawnDrivers: UIO[Fiber.Runtime[Throwable, Nothing]] =
    driverLoop.forever.forkDaemon

  private def replicate(term: Long, seq: Long, command: Array[Byte]): UIO[Unit] =
    peersRef.get.flatMap { peers =>
      ZIO.foreachParDiscard(peers) { case (peerId, client) =>
        client
          .appendEntries(
            AppendEntriesRequest(
              leaderId = nodeId,
              term = term,
              command = ByteString.copyFrom(command),
              seq = seq
            )
          )
          .foldZIO(
            _ => ZIO.unit,
            resp =>
              stepDownIfStale(resp.term) *>
                // A follower whose lastSeq diverges from ours rejects with
                // success=false; without a catch-up the strict seq check keeps
                // refusing every subsequent entry until a new election. Push a
                // snapshot to bring them in sync as long as we're still the
                // leader at this term.
                ZIO.when(!resp.success && resp.term <= term) {
                  sendSnapshotTo(peerId, client)
                }
          )
      }
    }

  private def sendSnapshotTo(peerId: String, client: DRefConsensusClient): UIO[Unit] =
    for {
      st       <- stateRef.get
      snapshot <- stateMachine.takeSnapshot
      _        <- client
                    .installSnapshot(
                      InstallSnapshotRequest(
                        leaderId = nodeId,
                        term = st.term,
                        snapshot = Some(snapshot),
                        lastSeq = st.lastSeq
                      )
                    )
                    .foldZIO(
                      _ => ZIO.unit,
                      resp => stepDownIfStale(resp.term)
                    )
                    .when(st.role == Role.Leader)
                    .unit
    } yield ()

  private val driverLoop: UIO[Unit] =
    for {
      currentRole <- role
      _           <- currentRole match {
                       case Role.Leader                    => sendHeartbeats
                       case Role.Follower | Role.Candidate =>
                         for {
                           now         <- Clock.nanoTime
                           st          <- stateRef.get
                           jitter      <- Random.nextLongBetween(
                                            0L,
                                            math.max(1L, config.electionTimeout.toMillis / 2)
                                          )
                           timeoutNanos = config.electionTimeout.toNanos + (jitter * 1_000_000L)
                           elapsed      = now - st.lastHeartbeatNanos
                           _           <- startElection.when(elapsed >= timeoutNanos)
                         } yield ()
                     }
      _           <- ZIO.sleep(config.heartbeatInterval)
    } yield ()

  private def sendHeartbeats: UIO[Unit] =
    stateRef.get.flatMap { st =>
      peersRef.get.flatMap { peers =>
        ZIO.foreachParDiscard(peers) { case (peerId, client) =>
          client
            .heartbeat(HeartbeatRequest(leaderId = nodeId, term = st.term))
            .foldZIO(
              _ => ZIO.unit,
              resp => stepDownIfStale(resp.term)
            )
        }
      }
    }

  private def startElection: UIO[Unit] =
    for {
      election                   <- updateAndPersist { st =>
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
      _                          <- ZIO.logInfo(s"starting election on node $nodeId term $term")
      peers                      <- peersRef.get
      responses                  <- ZIO.foreachPar(peers.toList) { case (peerId, client) =>
                                      client
                                        .requestVote(VoteRequest(candidateId = nodeId, term = term, lastSeq = lastSeq))
                                        .map(Some(_))
                                        .catchAll { _ =>
                                          ZIO.logDebug(s"vote request failed for peer $peerId") *> ZIO.none
                                        }
                                    }
      higherTerm                  = responses.flatten.collect { case resp if resp.term > term => resp.term }.headOption
      _                          <- higherTerm match {
                                      case Some(newTerm) =>
                                        updateAndPersist { st =>
                                          if newTerm > st.term then ((), st.copy(term = newTerm, role = Role.Follower, votedFor = None))
                                          else ((), st)
                                        }
                                      case None          =>
                                        val votes = 1 + responses.flatten.count(_.granted)
                                        val needed = (peers.size + 1) / 2 + 1
                                        if votes >= needed then
                                          stateRef
                                            .modify { st =>
                                              if st.role == Role.Candidate && st.term == term then
                                                val next = st.copy(role = Role.Leader, leaderId = Some(nodeId))
                                                (true, next)
                                              else (false, st)
                                            }
                                            .flatMap { elected =>
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
      _        <- peersRef.get.flatMap { peers =>
                    ZIO.foreachParDiscard(peers) { case (peerId, client) =>
                      client
                        .installSnapshot(
                          InstallSnapshotRequest(
                            leaderId = nodeId,
                            term = st.term,
                            snapshot = Some(snapshot),
                            lastSeq = st.lastSeq
                          )
                        )
                        .foldZIO(
                          _ => ZIO.unit,
                          resp => stepDownIfStale(resp.term)
                        )
                    }
                  }
    } yield ()

  /** Step down to follower if an outgoing heartbeat / append / snapshot response carries a term greater than ours.
    * Without this step a stale leader can stay convinced it is in charge while a follower has sprinted ahead (e.g.
    * through repeated election timeouts during a partition).
    */
  private def stepDownIfStale(observedTerm: Long): UIO[Unit] =
    updateAndPersist { st =>
      if observedTerm > st.term then
        (
          (),
          st.copy(
            term = observedTerm,
            role = Role.Follower,
            votedFor = None,
            leaderId = None,
            lastHeartbeatNanos = java.lang.System.nanoTime()
          )
        )
      else ((), st)
    }
}

object ProtoConsensusEngine {

  def make(
    nodeId: String,
    stateMachine: ProtoStateMachine,
    config: ProtoRaftConfig
  ): ZIO[Scope, Throwable, ProtoConsensusEngine] =
    for {
      peerEntries    <- ZIO.foreach(config.initialEndpoints.filter(_.id != nodeId)) { ep =>
                          DRefConsensusClient
                            .scoped(GrpcChannels.managedChannel(ep.address))
                            .map(ep.id -> _)
                        }
      peers           = peerEntries.toMap
      voterStore     <- config.storageDir match {
                          case Some(path) => VoterStateStore.file(path)
                          case None       => ZIO.succeed(VoterStateStore.noop)
                        }
      loaded         <- voterStore.load
      // When persistent state already exists, never short-circuit to leader:
      // doing so would skip the election protocol and the persisted term/vote
      // would no longer match a fresh leader's claim. Let the standard
      // election path run and increment the term cleanly.
      hasPersisted    = loaded.term > 0L || loaded.votedFor.isDefined
      initialRole     = if peers.isEmpty && !hasPersisted then Role.Leader else Role.Follower
      initialLeader   = if initialRole == Role.Leader then Some(nodeId) else None
      initialTerm     = if initialRole == Role.Leader then 1L else loaded.term
      initialVotedFor = if initialRole == Role.Leader then None else loaded.votedFor
      stateRef       <- Ref.make(
                          ConsensusState(
                            role = initialRole,
                            term = initialTerm,
                            votedFor = initialVotedFor,
                            leaderId = initialLeader,
                            lastSeq = 0L,
                            lastHeartbeatNanos = java.lang.System.nanoTime()
                          )
                        )
      // If we took the single-node leader shortcut, fsync the bootstrap term
      // immediately so a crash before the first election still leaves us at
      // a term we can compare against on restart.
      _              <- voterStore
                          .save(VoterState(initialTerm, initialVotedFor))
                          .when(initialTerm != loaded.term || initialVotedFor != loaded.votedFor)
      persistMutex   <- Semaphore.make(1)
      peersRef       <- Ref.make(peers)
    } yield new ProtoConsensusEngine(nodeId, stateMachine, peersRef, config, stateRef, voterStore, persistMutex)

  def waitForLeader(engine: ProtoConsensusEngine, max: Duration): UIO[Option[String]] =
    ZStream
      .repeatZIOWithSchedule(engine.leaderId, Schedule.spaced(50.millis))
      .collect { case Some(id) => id }
      .runHead
      .timeout(max)
      .map(_.flatten)
}
