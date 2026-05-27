package io.github.tobia80.dref.raft.proto

import zio.*

import java.io.{EOFException, FileOutputStream, IOException}
import java.nio.file.{Files, Path, StandardCopyOption}

final case class VoterState(term: Long, votedFor: Option[String])

object VoterState {
  val empty: VoterState = VoterState(0L, None)
}

/** Persists Raft voter state (currentTerm, votedFor) across restarts.
  *
  * Without this, a restarted node can grant a second vote in the same term and cause a split-brain — the one Raft
  * safety invariant that *requires* stable storage even before any log persistence work.
  */
trait VoterStateStore {
  def load: Task[VoterState]
  def save(state: VoterState): Task[Unit]
}

object VoterStateStore {
  private val FileName = "voter-state"
  private val TmpSuffix = ".tmp"

  /** In-memory no-op store. Returns empty state and discards writes — used when no `storageDir` is configured,
    * preserving the previous in-memory behavior bit-for-bit.
    */
  val noop: VoterStateStore = new VoterStateStore {
    def load: Task[VoterState] = ZIO.succeed(VoterState.empty)
    def save(state: VoterState): Task[Unit] = ZIO.unit
  }

  def file(dir: Path): Task[VoterStateStore] =
    ZIO
      .attemptBlocking(Files.createDirectories(dir))
      .as(new FileStore(dir))

  final private class FileStore(dir: Path) extends VoterStateStore {
    private val target = dir.resolve(FileName)
    private val tmp = dir.resolve(FileName + TmpSuffix)

    def load: Task[VoterState] =
      ZIO.attemptBlocking {
        if !Files.exists(target) then VoterState.empty
        else
          try RaftStorageCodec.decodeVoterState(Files.readAllBytes(target))
          catch case _: EOFException => throw new IOException(s"voter-state file truncated: $target")
      }

    def save(state: VoterState): Task[Unit] =
      ZIO.attemptBlocking {
        val out = new FileOutputStream(tmp.toFile)
        try {
          out.write(RaftStorageCodec.encodeVoterState(state))
          out.flush()
          out.getFD.sync()
        } finally out.close()
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        ()
      }
  }
}
