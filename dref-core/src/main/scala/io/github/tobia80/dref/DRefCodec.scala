package io.github.tobia80.dref

import zio.{Chunk, Task}

trait DRefCodec[T] {

  def serialize(value: T): Task[Array[Byte]]

  def deserialize(data: Chunk[Byte]): Task[T]

}

object DRefCodec {

  def serializeToArray[T: DRefCodec](value: T): Task[Array[Byte]] = implicitly[DRefCodec[T]].serialize(value)

  def deserializeFromArray[T: DRefCodec](data: Array[Byte]): Task[T] =
    implicitly[DRefCodec[T]].deserialize(Chunk.fromArray(data))

}
