package io.github.tobia80.dref.raft.proto

import io.github.tobia80.dref_consensus.ClusterSnapshot

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream,
  EOFException,
  IOException
}
import java.nio.charset.StandardCharsets

/** Shared on-disk / database wire formats for Raft persistence stores. */
private[proto] object RaftStorageCodec {

  val VoterStateMagic: Int = 0x44524654 // "DRFT"
  val SnapshotMagic: Int = 0x44524653 // "DRFS"
  val Version: Byte = 1

  def encodeVoterState(state: VoterState): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val data = new DataOutputStream(out)
    writeVoterState(data, state)
    data.flush()
    out.toByteArray
  }

  def decodeVoterState(bytes: Array[Byte]): VoterState = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try readVoterState(in)
    catch case _: EOFException => throw new IOException("voter-state payload truncated")
  }

  def encodeSnapshot(snapshot: ClusterSnapshot): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val data = new DataOutputStream(out)
    writeSnapshot(data, snapshot)
    data.flush()
    out.toByteArray
  }

  def decodeSnapshot(bytes: Array[Byte]): ClusterSnapshot = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try readSnapshot(in)
    catch case _: EOFException => throw new IOException("state-snapshot payload truncated")
  }

  private def readVoterState(in: DataInputStream): VoterState = {
    val magic = in.readInt()
    if magic != VoterStateMagic then
      throw new IOException(f"voter-state magic mismatch: expected 0x${VoterStateMagic}%08x got 0x$magic%08x")
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

  private def writeVoterState(out: DataOutputStream, state: VoterState): Unit = {
    out.writeInt(VoterStateMagic)
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

  private def readSnapshot(in: DataInputStream): ClusterSnapshot = {
    val magic = in.readInt()
    if magic != SnapshotMagic then
      throw new IOException(f"state-snapshot magic mismatch: expected 0x${SnapshotMagic}%08x got 0x$magic%08x")
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
    out.writeInt(SnapshotMagic)
    out.writeByte(Version)
    out.writeInt(payload.length)
    out.write(payload)
  }
}
