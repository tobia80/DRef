package io.github.tobia80.dref.raft.proto

import zio.*

import java.io.{DataInputStream, DataOutputStream, EOFException, FileInputStream, FileOutputStream, IOException, RandomAccessFile}
import java.nio.file.{Files, Path, StandardCopyOption}

/** One persisted command log entry keyed by monotonic sequence number. */
final case class CommandLogEntry(seq: Long, command: Array[Byte])

/** Loaded command log state: durable commit index plus all stored entries. */
final case class CommandLogState(commitSeq: Long, entries: Map[Long, Array[Byte]])

object CommandLogState {
  val empty: CommandLogState = CommandLogState(0L, Map.empty)
}

/** Append-only command log persisted before replication acks.
  *
  * Wire format (big-endian, shared with the Rust port):
  *   header: magic "DRFL" (4) | version 1 (1) | commit_seq u64 (8)
  *   record: seq u64 (8) | command_len i32 (4) | command bytes
  */
trait CommandLogStore {
  def load: Task[CommandLogState]
  def append(seq: Long, command: Array[Byte]): Task[Unit]
  def setCommitSeq(commitSeq: Long): Task[Unit]
  /** Drop every record with `seq <= throughSeq` and clamp the header commit index. */
  def truncateThrough(throughSeq: Long): Task[Unit]
  /** Drop every record with `seq >= fromSeq`. Used to roll back a failed append; the header
    * commit index is left alone (a failed append never advanced it).
    */
  def truncateFrom(fromSeq: Long): Task[Unit]
}

object CommandLogStore {
  private val Magic: Int = 0x4452464c // "DRFL"
  private val Version: Byte = 1
  private val HeaderSize: Int = 13
  private val FileName = "command-log"
  private val TmpSuffix = ".tmp"

  val noop: CommandLogStore = new CommandLogStore {
    def load: Task[CommandLogState] = ZIO.succeed(CommandLogState.empty)
    def append(seq: Long, command: Array[Byte]): Task[Unit] = ZIO.unit
    def setCommitSeq(commitSeq: Long): Task[Unit] = ZIO.unit
    def truncateThrough(throughSeq: Long): Task[Unit] = ZIO.unit
    def truncateFrom(fromSeq: Long): Task[Unit] = ZIO.unit
  }

  def file(dir: Path): Task[CommandLogStore] =
    ZIO
      .attemptBlocking(Files.createDirectories(dir))
      .as(new FileStore(dir))

  final private class FileStore(dir: Path) extends CommandLogStore {
    private val target = dir.resolve(FileName)
    private val tmp = dir.resolve(FileName + TmpSuffix)

    def load: Task[CommandLogState] =
      ZIO.attemptBlocking {
        if !Files.exists(target) then CommandLogState.empty
        else {
          val in = new DataInputStream(new FileInputStream(target.toFile))
          try readLog(in)
          catch case _: EOFException => throw new IOException(s"command-log file truncated: $target")
          finally in.close()
        }
      }

    def append(seq: Long, command: Array[Byte]): Task[Unit] =
      ZIO.attemptBlocking {
        ensureHeader()
        val out = new FileOutputStream(target.toFile, true)
        try {
          val data = new DataOutputStream(out)
          data.writeLong(seq)
          data.writeInt(command.length)
          data.write(command)
          data.flush()
          out.getFD.sync()
        } finally out.close()
      }

    def setCommitSeq(commitSeq: Long): Task[Unit] =
      ZIO.attemptBlocking {
        ensureHeader()
        val raf = new RandomAccessFile(target.toFile, "rw")
        try {
          raf.seek(5L)
          raf.writeLong(commitSeq)
          raf.getFD.sync()
        } finally raf.close()
      }

    def truncateThrough(throughSeq: Long): Task[Unit] =
      ZIO.attemptBlocking {
        if !Files.exists(target) then ()
        else {
          val in = new DataInputStream(new FileInputStream(target.toFile))
          val loaded =
            try readLog(in)
            catch case _: EOFException => throw new IOException(s"command-log file truncated: $target")
            finally in.close()
          val kept = loaded.entries.filter { case (seq, _) => seq > throughSeq }
          val newCommit = math.min(loaded.commitSeq, throughSeq)
          rewrite(newCommit, kept)
        }
      }

    def truncateFrom(fromSeq: Long): Task[Unit] =
      ZIO.attemptBlocking {
        if !Files.exists(target) then ()
        else {
          val in = new DataInputStream(new FileInputStream(target.toFile))
          val loaded =
            try readLog(in)
            catch case _: EOFException => throw new IOException(s"command-log file truncated: $target")
            finally in.close()
          val kept = loaded.entries.filter { case (seq, _) => seq < fromSeq }
          rewrite(loaded.commitSeq, kept)
        }
      }

    private def ensureHeader(): Unit =
      if !Files.exists(target) then {
        val out = new FileOutputStream(target.toFile)
        try {
          val data = new DataOutputStream(out)
          data.writeInt(Magic)
          data.writeByte(Version)
          data.writeLong(0L)
          data.flush()
          out.getFD.sync()
        } finally out.close()
      }

    private def rewrite(commitSeq: Long, entries: Map[Long, Array[Byte]]): Unit = {
      val out = new FileOutputStream(tmp.toFile)
      try {
        val data = new DataOutputStream(out)
        data.writeInt(Magic)
        data.writeByte(Version)
        data.writeLong(commitSeq)
        entries.toList.sortBy(_._1).foreach { case (seq, command) =>
          data.writeLong(seq)
          data.writeInt(command.length)
          data.write(command)
        }
        data.flush()
        out.getFD.sync()
      } finally out.close()
      Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      ()
    }
  }

  private def readLog(in: DataInputStream): CommandLogState = {
    val magic = in.readInt()
    if magic != Magic then
      throw new IOException(f"command-log magic mismatch: expected 0x${Magic}%08x got 0x$magic%08x")
    val version = in.readByte()
    if version != Version then throw new IOException(s"command-log version $version not supported")
    val commitSeq = in.readLong()
    val entries = scala.collection.mutable.Map.empty[Long, Array[Byte]]
    try
      while true do
        val seq = in.readLong()
        val len = in.readInt()
        if len < 0 then throw new IOException(s"command-log negative command length at seq $seq")
        val command = new Array[Byte](len)
        in.readFully(command)
        entries.update(seq, command)
    catch case _: EOFException => ()
    CommandLogState(commitSeq, entries.toMap)
  }

  /** Encode a single record for golden-vector tests (header + one entry). */
  def encodeForTest(entry: CommandLogEntry, commitSeq: Long = 0L): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val data = new DataOutputStream(out)
    data.writeInt(Magic)
    data.writeByte(Version)
    data.writeLong(commitSeq)
    data.writeLong(entry.seq)
    data.writeInt(entry.command.length)
    data.write(entry.command)
    data.flush()
    out.toByteArray
  }
}
