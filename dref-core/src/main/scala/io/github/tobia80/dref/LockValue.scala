package io.github.tobia80.dref

import java.nio.ByteBuffer

/** Wire format for distributed lock tokens. Both Scala and Rust clients
  * store lock ownership as an 8-byte big-endian `Long` so mixed-language
  * clusters can detect stolen locks consistently.
  */
object LockValue {

  def toBytes(value: Long): Array[Byte] = {
    val buf = ByteBuffer.allocate(8)
    buf.putLong(value)
    buf.array()
  }

  def fromBytes(bytes: Array[Byte]): Long =
    ByteBuffer.wrap(bytes).getLong()
}
