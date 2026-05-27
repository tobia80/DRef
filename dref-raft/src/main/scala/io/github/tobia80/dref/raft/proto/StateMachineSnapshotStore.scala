package io.github.tobia80.dref.raft.proto

import io.github.tobia80.dref_consensus.ClusterSnapshot
import zio.*

import java.io.{DataInputStream, DataOutputStream, EOFException, FileInputStream, FileOutputStream, IOException}
import java.nio.file.{Files, Path, StandardCopyOption}

/** Persists the Raft state machine's [[ClusterSnapshot]] to disk.
  *
  * The voter state store ([[VoterStateStore]]) makes the node Raft-safe across restarts; this store closes the
  * companion gap on the *application* side. Without it, a restarted node boots with an empty in-memory state and has
  * to be re-seeded by the leader's `InstallSnapshot` RPC. That works, but it adds avoidable network cost and recovery
  * time on every restart — especially painful for clusters with large key sets or thin leader↔follower bandwidth.
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
  private val Magic: Int = 0x44524653 // "DRFS"
  private val Version: Byte = 1
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
        else {
          val in = new DataInputStream(new FileInputStream(target.toFile))
          try Some(readSnapshot(in))
          catch case _: EOFException => throw new IOException(s"state-snapshot file truncated: $target")
          finally in.close()
        }
      }

    def save(snapshot: ClusterSnapshot): Task[Unit] =
      ZIO.attemptBlocking {
        val out = new FileOutputStream(tmp.toFile)
        try {
          val data = new DataOutputStream(out)
          writeSnapshot(data, snapshot)
          data.flush()
          out.getFD.sync()
        } finally out.close()
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        ()
      }
  }

  private def readSnapshot(in: DataInputStream): ClusterSnapshot = {
    val magic = in.readInt()
    if magic != Magic then
      throw new IOException(f"state-snapshot magic mismatch: expected 0x${Magic}%08x got 0x$magic%08x")
    val version = in.readByte()
    if version != Version then throw new IOException(s"state-snapshot version $version not supported")
    val len = in.readInt()
    if len < 0 then throw new IOException(s"state-snapshot length negative: $len")
    val payload = new Array[Byte](len)
    in.readFully(payload)
    ClusterSnapshot.parseFrom(payload)
  }

  private def writeSnapshot(out: DataOutputStream, snapshot: ClusterSnapshot): Unit = {
    val payload = snapshot.toByteArray
    out.writeInt(Magic)
    out.writeByte(Version)
    out.writeInt(payload.length)
    out.write(payload)
  }
}
