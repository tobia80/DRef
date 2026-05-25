package io.github.tobia80.dref.raft.proto

import zio.*

import java.io.{DataInputStream, DataOutputStream, EOFException, FileInputStream, FileOutputStream, IOException}
import java.nio.charset.StandardCharsets
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
  private val Magic: Int = 0x44524654 // "DRFT"
  private val Version: Byte = 1
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
        else {
          val in = new DataInputStream(new FileInputStream(target.toFile))
          try readState(in)
          catch case _: EOFException => throw new IOException(s"voter-state file truncated: $target")
          finally in.close()
        }
      }

    def save(state: VoterState): Task[Unit] =
      ZIO.attemptBlocking {
        val out = new FileOutputStream(tmp.toFile)
        try {
          val data = new DataOutputStream(out)
          writeState(data, state)
          data.flush()
          out.getFD.sync()
        } finally out.close()
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        ()
      }
  }

  private def readState(in: DataInputStream): VoterState = {
    val magic = in.readInt()
    if magic != Magic then
      throw new IOException(f"voter-state magic mismatch: expected 0x${Magic}%08x got 0x$magic%08x")
    val version = in.readByte()
    if version != Version then throw new IOException(s"voter-state version $version not supported")
    val term = in.readLong()
    val len = in.readInt()
    val votedFor =
      if len < 0 then None
      else {
        val bytes = new Array[Byte](len)
        in.readFully(bytes)
        Some(new String(bytes, StandardCharsets.UTF_8))
      }
    VoterState(term, votedFor)
  }

  private def writeState(out: DataOutputStream, state: VoterState): Unit = {
    out.writeInt(Magic)
    out.writeByte(Version)
    out.writeLong(state.term)
    state.votedFor match {
      case None     => out.writeInt(-1)
      case Some(id) =>
        val bytes = id.getBytes(StandardCharsets.UTF_8)
        out.writeInt(bytes.length)
        out.write(bytes)
    }
  }
}
