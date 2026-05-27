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
  commitSeq: Long,
  lastApplied: Long,
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

  /** Returned when the leader cannot replicate a write to a majority of the cluster. */
  case object QuorumLost extends ConsensusError {
    override def getMessage: String = "write could not be replicated to a quorum"
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
  pendingRef: Ref[Map[Long, Array[Byte]]],
  voterStore: VoterStateStore,
  commandLogStore: CommandLogStore,
  persistMutex: Semaphore,
  logMutex: Semaphore,
  applyMutex: Semaphore,
  snapshotStore: StateMachineSnapshotStore,
  snapshotMutex: Semaphore,
  appliesSinceSnapshot: Ref[Long]
) {

  /** Align consensus replication peers with the current discovery snapshot. */
  def syncPeers(
    endpoints: List[NodeEndpoint],
    selfNodeId: String,
    bindAddress: String
  ): ZIO[Scope, Throwable, Unit] =
    val desired = endpoints.filter(ep => ep.id != selfNodeId && ep.address != bindAddress)
    PeerMapSync.sync(
      peersRef,
      desired,
      ep => DRefConsensusClient.scoped(GrpcChannels.managedChannel(ep.address))
    )

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

  private def withCommandLog[A](op: CommandLogStore => Task[A]): UIO[A] =
    logMutex.withPermit(op(commandLogStore).orDie)

  /** Record that a command was successfully applied. When the running tally crosses the configured
    * `snapshotEvery` threshold, fork off a snapshot write so the on-disk picture catches up.
    *
    * The write is forked rather than awaited: snapshotting is a *durability* optimisation, not a correctness
    * requirement, so a slow disk must not stretch out replication latency. If the write fails we log and reset the
    * counter anyway — retrying in a tight loop would just amplify the underlying disk problem.
    */
  private def noteApplied: UIO[Unit] =
    if config.snapshotEvery <= 0 then ZIO.unit
    else
      appliesSinceSnapshot.modify { n =>
        val next = n + 1L
        if next >= config.snapshotEvery.toLong then (true, 0L) else (false, next)
      }.flatMap { shouldSnapshot =>
        ZIO.when(shouldSnapshot)(persistSnapshotInBackground).unit
      }

  private def persistSnapshotInBackground: UIO[Unit] =
    persistSnapshotNow.catchAll { t =>
      ZIO.logWarningCause(s"state-machine snapshot save failed on node $nodeId", Cause.fail(t))
    }.forkDaemon.unit

  /** Take a snapshot of the state machine and fsync it to disk. Serialised so concurrent triggers don't both write —
    * the second waits for the first, then takes a fresh snapshot itself.
    */
  private def persistSnapshotNow: Task[Unit] =
    snapshotMutex.withPermit {
      for {
        st       <- stateRef.get
        snapshot <- stateMachine.takeSnapshot
        // lastApplied — NOT lastSeq — is the highest seq actually reflected in the state
        // machine. On a follower, lastSeq can race ahead of lastApplied via AppendEntries
        // before the commit catches up; saving lastSeq would make the snapshot file lie
        // about what's been applied and silently lose entries on restart.
        _        <- snapshotStore.save(snapshot.copy(lastSeq = st.lastApplied))
        _        <- withCommandLog(_.truncateThrough(st.lastApplied))
      } yield ()
    }

  /** Force a snapshot persist now. Exposed for tests and graceful shutdown. */
  def takeAndPersistSnapshot: Task[Unit] = persistSnapshotNow
  def role: UIO[Role] = stateRef.get.map(_.role)

  def isLeader: UIO[Boolean] = role.map(_ == Role.Leader)

  def leaderId: UIO[Option[String]] = stateRef.get.map(_.leaderId)

  /** Current Raft term as this node sees it. Exposed for tests that assert the term does not spike across cluster
    * events (e.g. follower restart with PreVote enabled).
    */
  def currentTerm: UIO[Long] = stateRef.get.map(_.term)

  /** Apply pending entries up to `commitSeq` and advance the committed/applied markers.
    *
    * Serialised on `applyMutex` so concurrent inbound RPCs (e.g. a heartbeat racing an
    * AppendEntries with a larger commitSeq) cannot both pass the guard and double-apply the
    * same pending entry.
    *
    * The effective commit index is capped to `lastSeq` — a leader may legitimately advertise
    * a commitSeq past entries we haven't received yet, and we must not advance our on-disk
    * commit past entries that aren't in our log. If a pending entry is still missing within
    * the effective range (a concurrent AppendEntries bumped lastSeq but hasn't yet inserted
    * the bytes), we stop at the last successfully applied seq; the next call retries.
    */
  private def applyCommitted(commitSeq: Long): UIO[Unit] =
    applyMutex.withPermit {
      stateRef.get.flatMap { st =>
        val effective = math.min(commitSeq, st.lastSeq)
        if effective <= st.commitSeq then ZIO.unit
        else applyLoop(st.lastApplied + 1L, effective)
      }
    }

  private def applyLoop(start: Long, end: Long): UIO[Unit] = {
    def advanceCommit(lastSuccess: Long): UIO[Unit] =
      if lastSuccess >= start then
        stateRef.update(_.copy(commitSeq = lastSuccess)) *>
          withCommandLog(_.setCommitSeq(lastSuccess))
      else ZIO.unit

    def go(seq: Long, lastSuccess: Long): UIO[Unit] =
      if seq > end then advanceCommit(lastSuccess)
      else
        pendingRef.get.flatMap { pending =>
          pending.get(seq) match {
            case Some(value) =>
              for {
                cmd <- ZIO.attempt(StateCommand.parseFrom(value)).orDie
                _   <- stateMachine.apply(cmd)
                _   <- stateRef.update(s => s.copy(lastApplied = seq))
                _   <- pendingRef.update(_ - seq)
                // noteApplied runs *after* lastApplied is updated so a forked snapshot
                // reads a consistent (lastApplied, state-machine) pair.
                _   <- noteApplied
                _   <- go(seq + 1L, seq)
              } yield ()
            case None =>
              ZIO.logWarning(s"pending entry for committed seq $seq not yet present; deferring") *>
                advanceCommit(lastSuccess)
          }
        }

    go(start, start - 1L)
  }

  /** Push the current commit index to every follower so they apply pending entries. */
  private def propagateCommitSeq(commitSeq: Long, term: Long): UIO[Unit] =
    peersRef.get.flatMap { peers =>
      ZIO.foreachParDiscard(peers) { case (_, client) =>
        client
          .heartbeat(HeartbeatRequest(leaderId = nodeId, term = term, commitSeq = commitSeq))
          .foldZIO(_ => ZIO.unit, resp => stepDownIfStale(resp.term))
      }
    }

  private def quorumNeeded(clusterSize: Int): Int = clusterSize / 2 + 1

  private def stepToFollower(st: ConsensusState, term: Long, leaderId: String): ConsensusState =
    val stepped = if term > st.term then st.copy(term = term, votedFor = None) else st
    stepped.copy(
      role = Role.Follower,
      leaderId = Some(leaderId),
      lastHeartbeatNanos = java.lang.System.nanoTime()
    )

  private def followLeaderOrReject(
    st: ConsensusState,
    leaderId: String,
    term: Long
  )(whenFollowing: ConsensusState => (Boolean, ConsensusState)): ((Boolean, Long), ConsensusState) =
    if term < st.term then ((false, st.term), st)
    else
      val next = stepToFollower(st, term, leaderId)
      val (accepted, updated) = whenFollowing(next)
      ((accepted, updated.term), updated)

  def submit(cmd: StateCommand): IO[ConsensusError, ApplyResult] =
    for {
      termAndSeq <- stateRef
                      .modify { st =>
                        if st.role != Role.Leader then (Left(ConsensusError.NotLeader(st.leaderId)), st)
                        else
                          val next = st.copy(lastSeq = st.lastSeq + 1)
                          (Right((next.term, next.lastSeq, next.commitSeq)), next)
                      }
                      .flatMap {
                        case Left(err)   => ZIO.fail(err)
                        case Right(trip) => ZIO.succeed(trip)
                      }
      (term, seq, commitSeqBefore) = termAndSeq
      bytes                        = cmd.toByteArray
      _                           <- withCommandLog(_.append(seq, bytes))
      peers                       <- peersRef.get
      needed                       = quorumNeeded(peers.size + 1)
      followerAcks                <- replicateForQuorum(term, seq, bytes, commitSeqBefore)
      totalAcks                    = 1 + followerAcks
      _                           <- ZIO.when(totalAcks < needed) {
                                       withCommandLog(_.truncateFrom(seq)) *>
                                         stateRef.update(_.copy(lastSeq = seq - 1)) *>
                                         ZIO.fail(ConsensusError.QuorumLost)
                                     }
      stillLeader                 <- isLeader
      _                           <- ZIO.fail(ConsensusError.NotLeader(None)).when(!stillLeader)
      result                      <- stateMachine.apply(cmd)
      _                           <- noteApplied
      _                           <- stateRef.update(s => s.copy(commitSeq = seq, lastApplied = seq))
      _                           <- withCommandLog(_.setCommitSeq(seq))
      _                           <- propagateCommitSeq(seq, term)
    } yield result

  /** Confirm leadership with a quorum before serving a linearizable read. */
  def readIndex: IO[ConsensusError, Unit] =
    for {
      st <- stateRef.get
      _  <- ZIO.fail(ConsensusError.NotLeader(st.leaderId)).when(st.role != Role.Leader)
      peers     <- peersRef.get
      needed     = quorumNeeded(peers.size + 1)
      responses <- ZIO.foreachPar(peers.toList) { case (peerId, client) =>
                     client
                       .readIndex(ReadIndexRequest(leaderId = nodeId, term = st.term))
                       .map(Some(_))
                       .catchAll { _ =>
                         ZIO.logDebug(s"read-index request failed for peer $peerId") *> ZIO.none
                       }
                   }
      higherTerm = responses.flatten.collect { case resp if resp.term > st.term => resp.term }.headOption
      _         <- higherTerm match {
                     case Some(newTerm) => stepDownIfStale(newTerm) *> ZIO.fail(ConsensusError.NotLeader(None))
                     case None          =>
                       val grants = 1 + responses.flatten.count(_.granted)
                       ZIO.fail(ConsensusError.NotLeader(st.leaderId)).when(grants < needed).unit
                   }
      applied <- stateRef.get.map(_.lastApplied)
      _       <- ZIO
                   .fail(ConsensusError.NotLeader(st.leaderId))
                   .when(applied < st.commitSeq)
    } yield ()

  def handleAppendEntries(
    leaderId: String,
    term: Long,
    seq: Long,
    command: Array[Byte],
    commitSeq: Long
  ): UIO[(Boolean, Long)] =
    for {
      updated                <- updateAndPersist { st =>
                                  followLeaderOrReject(st, leaderId, term) { next =>
                                    if seq != next.lastSeq + 1 then (false, next)
                                    else (true, next.copy(lastSeq = seq))
                                  }
                                }
      (accepted, currentTerm) = updated
      result                 <-
        if accepted then
          withCommandLog(_.append(seq, command)) *>
            pendingRef.update(_ + (seq -> command)) *>
            applyCommitted(commitSeq).as((true, currentTerm))
        else ZIO.succeed((false, currentTerm))
    } yield result

  def handleHeartbeat(leaderId: String, term: Long, commitSeq: Long): UIO[(Boolean, Long)] =
    for {
      updated <- updateAndPersist { st =>
                   followLeaderOrReject(st, leaderId, term)((next) => (true, next))
                 }
      (acknowledged, currentTerm) = updated
      _                          <- applyCommitted(commitSeq).when(acknowledged)
    } yield (acknowledged, currentTerm)

  /** Follower ack for a ReadIndex probe — confirms the requester is still the leader we know. */
  def handleReadIndex(leaderId: String, term: Long): UIO[(Boolean, Long)] =
    stateRef.get.map { st =>
      if term < st.term then (false, st.term)
      else if st.leaderId.contains(leaderId) && term == st.term then (true, st.term)
      else (false, st.term)
    }

  /** Handle a PreVote request (Ongaro thesis §9.6).
    *
    * PreVote is a "would-you-vote-for-me" probe a candidate runs BEFORE it actually bumps its term. The voter:
    *   1. does NOT change its own `term` / `votedFor` — granting a PreVote is hypothetical, so there is nothing to fsync;
    *   2. refuses if it has heard from a leader within the election timeout — that is the disruption guard PreVote
    *      exists for, since a partitioned node that kept incrementing its term in isolation must not be able to force
    *      a real election on rejoin;
    *   3. otherwise grants iff `lastSeq` is at least as up-to-date as ours AND the proposed term strictly beats ours.
    *
    * The returned term is always our current term — the voter never adopts the candidate's hypothetical term.
    */
  def handlePreVote(candidateId: String, term: Long, lastSeq: Long): UIO[(Boolean, Long)] =
    for {
      now      <- ZIO.succeed(java.lang.System.nanoTime())
      st       <- stateRef.get
      response  =
        if term <= st.term then (false, st.term)
        else
          // A node still in the Leader role refuses pre-votes outright — granting one would amount to volunteering
          // its own demotion. A leader that has gone stale only learns so via the term carried back on an
          // AppendEntries/Heartbeat response; until that signal arrives it trusts its own role. For followers, the
          // recency check on the last heartbeat plays the equivalent role: if we've heard from a leader within the
          // election timeout, the cluster is healthy and we shouldn't help an isolated candidate disrupt it.
          val elapsedNanos    = now - st.lastHeartbeatNanos
          val leaderRecent    = st.leaderId.isDefined && elapsedNanos < config.electionTimeout.toNanos
          val isActiveLeader  = st.role == Role.Leader
          if isActiveLeader || leaderRecent then (false, st.term)
          else
            val upToDate = lastSeq >= st.lastSeq
            (upToDate, st.term)
    } yield response

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
                                  followLeaderOrReject(st, leaderId, term) { next =>
                                    (true, next.copy(lastSeq = lastSeq))
                                  }
                                }
      (accepted, currentTerm) = updated
      _                      <- ZIO
                                  .when(accepted) {
                                    // Installing a snapshot replaces the whole state machine. Reset the
                                    // applies-counter and durably re-write the snapshot so a restart picks up the
                                    // freshly-received state instead of the leader's stale `lastSeq` gap.
                                    val durableSnapshot = snapshot.copy(lastSeq = lastSeq)
                                    withCommandLog(_.truncateThrough(lastSeq)) *>
                                      pendingRef.set(Map.empty) *>
                                      stateMachine.installSnapshot(durableSnapshot) *>
                                      stateRef.update(
                                        _.copy(commitSeq = lastSeq, lastApplied = lastSeq)
                                      ) *>
                                      appliesSinceSnapshot.set(0L) *>
                                      persistSnapshotInBackground
                                  }
    } yield (accepted, currentTerm)

  def spawnDrivers: UIO[Fiber.Runtime[Throwable, Nothing]] =
    driverLoop.forever.forkDaemon

  /** Replicate one entry and return the number of follower acks received. */
  private def replicateForQuorum(
    term: Long,
    seq: Long,
    command: Array[Byte],
    commitSeq: Long
  ): UIO[Int] =
    peersRef.get.flatMap { peers =>
      ZIO.foreachPar(peers.toList) { case (peerId, client) =>
        client
          .appendEntries(
            AppendEntriesRequest(
              leaderId = nodeId,
              term = term,
              command = ByteString.copyFrom(command),
              seq = seq,
              commitSeq = commitSeq
            )
          )
          .foldZIO(
            _ => ZIO.succeed(false),
            resp =>
              stepDownIfStale(resp.term) *>
                ZIO.when(!resp.success && resp.term <= term) {
                  sendSnapshotTo(peerId, client)
                }.as(resp.success)
          )
      }.map(_.count(identity))
    }

  private def installSnapshotToPeers(
    peers: Iterable[(String, DRefConsensusClient)],
    snapshot: ClusterSnapshot,
    term: Long,
    lastSeq: Long
  ): UIO[Unit] =
    ZIO.foreachParDiscard(peers) { case (_, client) =>
      client
        .installSnapshot(
          InstallSnapshotRequest(
            leaderId = nodeId,
            term = term,
            snapshot = Some(snapshot),
            lastSeq = lastSeq
          )
        )
        .foldZIO(_ => ZIO.unit, resp => stepDownIfStale(resp.term))
    }

  private def sendSnapshotTo(peerId: String, client: DRefConsensusClient): UIO[Unit] =
    for {
      st       <- stateRef.get
      snapshot <- stateMachine.takeSnapshot
      _        <- installSnapshotToPeers(
                    List(peerId -> client),
                    snapshot,
                    st.term,
                    st.lastApplied
                  ).when(st.role == Role.Leader)
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
        ZIO.foreachParDiscard(peers) { case (_, client) =>
          client
            .heartbeat(HeartbeatRequest(leaderId = nodeId, term = st.term, commitSeq = st.commitSeq))
            .foldZIO(
              _ => ZIO.unit,
              resp => stepDownIfStale(resp.term)
            )
        }
      }
    }

  /** Run a PreVote round. Returns `true` iff a quorum of peers indicate they would vote for us right now. Does not
    * mutate persisted state. A peer that responds with a strictly-greater term triggers a step-down — running a real
    * election would just lose against that higher-term holder anyway.
    */
  private def runPreVote: UIO[Boolean] =
    for {
      st            <- stateRef.get
      currentTerm    = st.term
      proposedTerm   = currentTerm + 1
      lastSeq        = st.lastSeq
      peers         <- peersRef.get
      needed         = quorumNeeded(peers.size + 1)
      responses     <- ZIO.foreachPar(peers.toList) { case (peerId, client) =>
                         client
                           .requestPreVote(
                             PreVoteRequest(candidateId = nodeId, term = proposedTerm, lastSeq = lastSeq)
                           )
                           .map(Some(_))
                           .catchAll { _ =>
                             ZIO.logDebug(s"pre-vote request failed for peer $peerId") *> ZIO.none
                           }
                       }
      higherTerm     = responses.flatten.collect { case resp if resp.term > currentTerm => resp.term }.headOption
      result        <- higherTerm match {
                         case Some(newTerm) => stepDownIfStale(newTerm).as(false)
                         case None          =>
                           val grants = 1 + responses.flatten.count(_.granted)
                           ZIO.succeed(grants >= needed)
                       }
    } yield result

  private def startElection: UIO[Unit] =
    for {
      passed                     <- runPreVote
      _                          <- if !passed then
                                      // Refresh the heartbeat clock so we don't immediately spin into another
                                      // pre-vote attempt on the next tick — the guard would just reject us again.
                                      stateRef.update(_.copy(lastHeartbeatNanos = java.lang.System.nanoTime())) *>
                                        ZIO.logDebug(s"pre-vote failed on node $nodeId; staying follower")
                                    else realElection
    } yield ()

  private def realElection: UIO[Unit] =
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
                                        if votes >= quorumNeeded(peers.size + 1) then
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
      peers    <- peersRef.get
      _        <- installSnapshotToPeers(peers, snapshot, st.term, st.lastApplied)
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
      snapshotStore  <- config.storageDir match {
                          case Some(path) => StateMachineSnapshotStore.file(path)
                          case None       => ZIO.succeed(StateMachineSnapshotStore.noop)
                        }
      commandLogStore <- config.storageDir match {
                          case Some(path) => CommandLogStore.file(path)
                          case None       => ZIO.succeed(CommandLogStore.noop)
                        }
      // Hydrate the state machine BEFORE peers come online so we don't serve
      // empty reads or accept appends against a stale lastSeq baseline. If the
      // snapshot file is corrupt or unreadable we fail-fast — silently booting
      // empty would diverge this node from the rest of the cluster.
      persisted      <- snapshotStore.load
      _              <- ZIO.foreachDiscard(persisted)(stateMachine.installSnapshot)
      snapshotLastSeq = persisted.fold(0L)(_.lastSeq)
      logState       <- commandLogStore.load
      maxLogSeq       = logState.entries.keys.maxOption.getOrElse(0L)
      recoveredCommit = math.min(logState.commitSeq, maxLogSeq)
      initialCommitSeq = math.max(snapshotLastSeq, recoveredCommit)
      initialLastSeq   = math.max(snapshotLastSeq, maxLogSeq)
      // A gap between the snapshot tip and the committed log range means we'd be
      // booting a state machine with missing committed entries — silent divergence.
      // Fail-fast so the operator sees the corruption and can restore from a peer.
      _              <- ZIO.foreachDiscard((snapshotLastSeq + 1L) to initialCommitSeq) { seq =>
                          logState.entries.get(seq) match {
                            case Some(bytes) =>
                              ZIO.attempt(StateCommand.parseFrom(bytes)).flatMap(stateMachine.apply).orDie
                            case None        =>
                              ZIO.dieMessage(
                                s"command log missing committed entry $seq (snapshotLastSeq=$snapshotLastSeq, commitSeq=$initialCommitSeq)"
                              )
                          }
                        }
      initialPending  = logState.entries.filter { case (s, _) => s > initialCommitSeq }
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
      restoredSeq     = initialLastSeq
      stateRef       <- Ref.make(
                          ConsensusState(
                            role = initialRole,
                            term = initialTerm,
                            votedFor = initialVotedFor,
                            leaderId = initialLeader,
                            lastSeq = restoredSeq,
                            commitSeq = initialCommitSeq,
                            lastApplied = initialCommitSeq,
                            lastHeartbeatNanos = java.lang.System.nanoTime()
                          )
                        )
      pendingRef     <- Ref.make(initialPending)
      // If we took the single-node leader shortcut, fsync the bootstrap term
      // immediately so a crash before the first election still leaves us at
      // a term we can compare against on restart.
      _              <- voterStore
                          .save(VoterState(initialTerm, initialVotedFor))
                          .when(initialTerm != loaded.term || initialVotedFor != loaded.votedFor)
      persistMutex   <- Semaphore.make(1)
      logMutex       <- Semaphore.make(1)
      applyMutex     <- Semaphore.make(1)
      snapshotMutex  <- Semaphore.make(1)
      appliesRef     <- Ref.make(0L)
      peersRef       <- Ref.make(peers)
    } yield new ProtoConsensusEngine(
      nodeId,
      stateMachine,
      peersRef,
      config,
      stateRef,
      pendingRef,
      voterStore,
      commandLogStore,
      persistMutex,
      logMutex,
      applyMutex,
      snapshotStore,
      snapshotMutex,
      appliesRef
    )

  def waitForLeader(engine: ProtoConsensusEngine, max: Duration): UIO[Option[String]] =
    ZStream
      .repeatZIOWithSchedule(engine.leaderId, Schedule.spaced(50.millis))
      .collect { case Some(id) => id }
      .runHead
      .timeout(max)
      .map(_.flatten)
}
