package io.github.tobia80.dref.raft.proto

import com.google.protobuf.ByteString
import io.github.tobia80.state_command.*

object StateCommands {

  def setElement(name: String, value: Array[Byte], expireAt: Option[Long]): StateCommand =
    StateCommand(
      StateCommand.Op.SetElement(
        SetElementCommand(name, ByteString.copyFrom(value), expireAt)
      )
    )

  def setElementIfNotExist(name: String, value: Array[Byte], expireAt: Option[Long]): StateCommand =
    StateCommand(
      StateCommand.Op.SetElementIfNotExist(
        SetElementIfNotExistCommand(name, ByteString.copyFrom(value), expireAt)
      )
    )

  def deleteElement(name: String): StateCommand =
    StateCommand(StateCommand.Op.DeleteElement(DeleteElementCommand(name)))

  def expireElement(name: String, expireAt: Long): StateCommand =
    StateCommand(StateCommand.Op.ExpireElement(ExpireElementCommand(name, expireAt)))

  def deleteIfExpired(name: String, now: Long): StateCommand =
    StateCommand(StateCommand.Op.DeleteIfExpired(DeleteIfExpiredCommand(name, now)))
}
