package io.github.tobia80.cref.raft

import io.github.tobia80.cref.*
import io.github.tobia80.raft.StartNewTermOpProto
import io.microraft.statemachine.StateMachine
import reactor.core.publisher.Sinks

import java.util
import java.util.function.Consumer

class CRefStateMachine(streamBuilder: Sinks.Many[ChangeEvent]) extends StateMachine {

  import scala.collection.mutable

  private val innerMap = mutable.Map[String, Array[Byte]]()

  override def runOperation(commitIndex: Long, operation: Any): AnyRef =
    operation match {
      case request: SetElementRequest           =>
        setElement(commitIndex, request)
      case request: SetElementIfNotExistRequest =>
        setElementIfNotExist(commitIndex, request)
      case request: GetElementRequest           => getElement(commitIndex, request).orNull
      case request: DeleteElementRequest        => deleteElement(commitIndex, request).orNull
      case _                                    =>
        throw new IllegalArgumentException(s"Unsupported operation: $operation")
    }

  private def setElement(commitIndex: Long, operation: SetElementRequest): AnyRef = {
    val res = innerMap.put(operation.name, operation.value.toByteArray)
    streamBuilder.tryEmitNext(SetElement(operation.name, operation.value.toByteArray)).orThrow()
    res
  }

  private def setElementIfNotExist(commitIndex: Long, operation: SetElementIfNotExistRequest): AnyRef = {
    val res = innerMap.get(operation.name)
    innerMap.put(operation.name, operation.value.toByteArray)
    streamBuilder.tryEmitNext(SetElement(operation.name, operation.value.toByteArray)).orThrow()
    java.lang.Boolean.valueOf(res.isEmpty) // if not exist set to true
  }

  private def getElement(commitIndex: Long, operation: GetElementRequest): Option[Array[Byte]] =
    innerMap.get(operation.name)

  private def deleteElement(commitIndex: Long, operation: DeleteElementRequest): Option[Array[Byte]] = {
    val res = innerMap.remove(operation.name)
    streamBuilder.tryEmitNext(DeleteElement(operation.name))
    res
  }

  override def takeSnapshot(commitIndex: Long, snapshotChunkConsumer: Consumer[AnyRef]): Unit =
    throw new UnsupportedOperationException("Snapshots not supported")

  override def installSnapshot(commitIndex: Long, snapshotChunks: util.List[AnyRef]): Unit =
    throw new UnsupportedOperationException("Snapshots not supported")

  override def getNewTermOperation: AnyRef = StartNewTermOpProto.defaultInstance
}
