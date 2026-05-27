package io.github.tobia80.dref.raft.proto

import io.github.tobia80.dref_consensus.ClusterSnapshot
import zio.*

import java.io.{EOFException, FileOutputStream, IOException}
import java.nio.file.{Files, Path, StandardCopyOption}

/** Persists the Raft state machine's [[ClusterSnapshot]] to disk.
  *
  * The voter state store ([[VoterStateStore]]) makes the node Raft-safe across restarts; this store closes the
  * companion gap on the *application* side. Without it, a restarted node boots with an empty in-memory state and has to
  * be re-seeded by the leader's `InstallSnapshot` RPC. That works, but it adds avoidable network cost and recovery time
  * on every restart — especially painful for clusters with large key sets or thin leader↔follower bandwidth.
  *
  * The on-disk format mirrors the voter-state file: a fixed magic prefix, a one-byte version, then the protobuf
  * payload. Writes go through a temp file + fsync + atomic rename, so a crash mid-write either leaves the previous
  * snapshot intact or no snapshot at all — never a half-written file that would surface as a `ParseFromException` on
  * reboot.
  */
trait StateMachineSnapshotStore {
  def load: Task[Option[ClusterSnapshot]]
  def save(snapshot: ClusterSnapshot): Task[Unit]
}

object StateMachineSnapshotStore {
  private val FileName = "state-snapshot"
  private val TmpSuffix = ".tmp"

  /** In-memory no-op store. `load` always reports "no snapshot", `save` discards. Used when no `storageDir` is
    * configured — preserves the previous behaviour exactly, where a fresh node always relied on the leader for state.
    */
  val noop: StateMachineSnapshotStore = new StateMachineSnapshotStore {
    def load: Task[Option[ClusterSnapshot]] = ZIO.none
    def save(snapshot: ClusterSnapshot): Task[Unit] = ZIO.unit
  }

  def file(dir: Path): Task[StateMachineSnapshotStore] =
    ZIO
      .attemptBlocking(Files.createDirectories(dir))
      .as(new FileStore(dir))

  final private class FileStore(dir: Path) extends StateMachineSnapshotStore {
    private val target = dir.resolve(FileName)
    private val tmp = dir.resolve(FileName + TmpSuffix)

    def load: Task[Option[ClusterSnapshot]] =
      ZIO.attemptBlocking {
        if !Files.exists(target) then None
        else
          try Some(RaftStorageCodec.decodeSnapshot(Files.readAllBytes(target)))
          catch case _: EOFException => throw new IOException(s"state-snapshot file truncated: $target")
      }

    def save(snapshot: ClusterSnapshot): Task[Unit] =
      ZIO.attemptBlocking {
        val out = new FileOutputStream(tmp.toFile)
        try {
          out.write(RaftStorageCodec.encodeSnapshot(snapshot))
          out.flush()
          out.getFD.sync()
        } finally out.close()
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        ()
      }
  }
}
