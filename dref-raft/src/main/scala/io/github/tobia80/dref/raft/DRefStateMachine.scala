package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.*
import io.github.tobia80.raft.{KVEntry, KVSnapshotChunkData, StartNewTermOpProto}
import io.microraft.statemachine.StateMachine
import reactor.core.publisher.Sinks

import java.util
import java.util.function.Consumer
import scala.jdk.CollectionConverters._

class DRefStateMachine(streamBuilder: Sinks.Many[ChangeEvent]) extends StateMachine {

  private case class ExpiringValue(value: Array[Byte], expireAt: Option[Long])

  private val innerMap = new java.util.concurrent.ConcurrentHashMap[String, ExpiringValue]()

  private def getOpt(key: String): Option[ExpiringValue] = Option(innerMap.get(key))

  override def runOperation(commitIndex: Long, operation: Any): AnyRef =
    operation match {
      case request: SetElementRequest           =>
        setElement(commitIndex, request)
      case request: SetElementIfNotExistRequest =>
        setElementIfNotExist(commitIndex, request)
      case request: GetElementRequest           => getElement(commitIndex, request).orNull
      case request: DeleteElementRequest        => deleteElement(commitIndex, request).orNull
      case request: DeleteIfExpiredRequest       => deleteIfExpired(commitIndex, request).orNull
      case request: ExpireElementRequest        => expireElement(commitIndex, request)
      case request: GetExpirationTableRequest   => retrieveExpirationTable(commitIndex, request)
      case request: StartNewTermOpProto         =>
        // No special handling needed for new term in this state machine
        null
      case _                                    =>
        throw new IllegalArgumentException(s"Unsupported operation: $operation")
    }

  private def retrieveExpirationTable(commitIndex: Long, request: GetExpirationTableRequest): Map[String, Long] =
    innerMap.asScala.collect { case (name, ExpiringValue(_, Some(expireAt))) =>
      (name, expireAt)
    }.toMap

  private def setElement(commitIndex: Long, operation: SetElementRequest): AnyRef = {
    innerMap.put(operation.name, ExpiringValue(operation.value.toByteArray, operation.expireAt))
    val _ = streamBuilder.tryEmitNext(SetElement(operation.name, operation.value.toByteArray))
    null
  }

  private def setElementIfNotExist(commitIndex: Long, operation: SetElementIfNotExistRequest): AnyRef =
    Option(
      innerMap.putIfAbsent(
        operation.name,
        ExpiringValue(operation.value.toByteArray, operation.expireAt)
      )
    ) match {
      case None =>
        val _ = streamBuilder.tryEmitNext(SetElement(operation.name, operation.value.toByteArray))
        java.lang.Boolean.TRUE
      case Some(_) => java.lang.Boolean.FALSE
    }

  private def getElement(commitIndex: Long, operation: GetElementRequest): Option[Array[Byte]] =
    getOpt(operation.name).map(_.value)

  private def deleteElement(commitIndex: Long, operation: DeleteElementRequest): Option[Array[Byte]] = {
    val res = innerMap.remove(operation.name)
    val _ = streamBuilder.tryEmitNext(DeleteElement(operation.name))
    Option(res).map(_.value)
  }

  private def deleteIfExpired(commitIndex: Long, operation: DeleteIfExpiredRequest): Option[Array[Byte]] =
    getOpt(operation.name).filter(_.expireAt.exists(_ <= operation.expiredBefore)).map { res =>
      innerMap.remove(operation.name)
      val _ = streamBuilder.tryEmitNext(DeleteElement(operation.name))
      res.value
    }

  private def expireElement(commitIndex: Long, operation: ExpireElementRequest): AnyRef =
    getOpt(operation.name) match {
      case Some(old) =>
        innerMap.put(operation.name, old.copy(expireAt = Some(operation.expireAt)))
        java.lang.Boolean.TRUE
      case None => java.lang.Boolean.FALSE
    }

  override def takeSnapshot(commitIndex: Long, snapshotChunkConsumer: Consumer[AnyRef]): Unit = {
    val values = for {
      (key, ev) <- innerMap.asScala.toSet
      kvEntry    = KVEntry(key, ByteString.copyFrom(ev.value), ev.expireAt)
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

  override def getNewTermOperation: AnyRef = StartNewTermOpProto()
}
