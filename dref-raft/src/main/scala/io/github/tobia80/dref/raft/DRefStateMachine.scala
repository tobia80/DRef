package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.*
import io.github.tobia80.raft.{KVEntry, KVSnapshotChunkData, StartNewTermOpProto}
import io.microraft.statemachine.StateMachine
import reactor.core.publisher.Sinks

import java.util
import java.util.function.Consumer

class DRefStateMachine(streamBuilder: Sinks.Many[ChangeEvent]) extends StateMachine {

  import scala.collection.mutable

  private case class ExpiringValue(value: Array[Byte], expireAt: Option[Long])

  private val innerMap = mutable.Map[String, ExpiringValue]()

  override def runOperation(commitIndex: Long, operation: Any): AnyRef =
    operation match {
      case request: SetElementRequest           =>
        setElement(commitIndex, request)
      case request: SetElementIfNotExistRequest =>
        setElementIfNotExist(commitIndex, request)
      case request: GetElementRequest           => getElement(commitIndex, request).orNull
      case request: DeleteElementRequest        => deleteElement(commitIndex, request).orNull
      case request: ExpireElementRequest        => expireElement(commitIndex, request)
      case request: GetExpirationTableRequest   => retrieveExpirationTable(commitIndex, request)
      case request: StartNewTermOpProto         =>
        // No special handling needed for new term in this state machine
        null
      case _                                    =>
        throw new IllegalArgumentException(s"Unsupported operation: $operation")
    }

  private def retrieveExpirationTable(commitIndex: Long, request: GetExpirationTableRequest) =
    innerMap.map { case (name, ExpiringValue(_, Some(expireAt))) =>
      (name, expireAt)
    }.toMap

  private def setElement(commitIndex: Long, operation: SetElementRequest): AnyRef = {
    val res = innerMap.put(operation.name, ExpiringValue(operation.value.toByteArray, operation.expireAt))
    streamBuilder.tryEmitNext(SetElement(operation.name, operation.value.toByteArray)).orThrow()
    res
  }

  private def setElementIfNotExist(commitIndex: Long, operation: SetElementIfNotExistRequest): AnyRef = {
    val res = innerMap.get(operation.name)
    res match {
      case Some(value) => java.lang.Boolean.FALSE
      case None        =>
        innerMap.put(operation.name, ExpiringValue(operation.value.toByteArray, operation.expireAt))
        streamBuilder.tryEmitNext(SetElement(operation.name, operation.value.toByteArray)).orThrow()
        java.lang.Boolean.TRUE
    }
  }

  private def getElement(commitIndex: Long, operation: GetElementRequest): Option[Array[Byte]] =
    innerMap.get(operation.name).map(_.value)

  private def deleteElement(commitIndex: Long, operation: DeleteElementRequest): Option[Array[Byte]] = {
    val res = innerMap.remove(operation.name)
    streamBuilder.tryEmitNext(DeleteElement(operation.name, false))
    res.map(_.value)
  }

  private def expireElement(commitIndex: Long, operation: ExpireElementRequest): AnyRef = {
    val res = innerMap.get(operation.name)
    res match {
      case Some(value) =>
        innerMap.put(operation.name, value.copy(expireAt = Some(operation.expireAt)))
        java.lang.Boolean.TRUE
      case None        => java.lang.Boolean.FALSE
    }
  }

  import scala.jdk.CollectionConverters.*

  override def takeSnapshot(commitIndex: Long, snapshotChunkConsumer: Consumer[AnyRef]): Unit = {
    val values = for {
      e      <- innerMap.toSet
      kvEntry = KVEntry(e._1, ByteString.copyFrom(e._2.value), e._2.expireAt)
    } yield kvEntry
    values.grouped(5).foreach { entry =>
      val chunk = KVSnapshotChunkData(entry.toSeq)
      snapshotChunkConsumer.accept(chunk)
    }
  }

  override def installSnapshot(commitIndex: Long, snapshotChunks: util.List[AnyRef]): Unit = {
    innerMap.clear()
    val values = for {
      chunk <- snapshotChunks.asScala
      entry <- chunk.asInstanceOf[KVSnapshotChunkData].entry
    } yield entry
    values.foreach(entry => innerMap.put(entry.key, ExpiringValue(entry.value.toByteArray, entry.expireAt)))
  }

  override def getNewTermOperation: AnyRef = StartNewTermOpProto.defaultInstance
}
