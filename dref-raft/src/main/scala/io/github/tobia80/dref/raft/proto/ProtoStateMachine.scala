package io.github.tobia80.dref.raft.proto

import com.google.protobuf.ByteString
import io.github.tobia80.dref.{ChangeEvent, DeleteElement, SetElement}
import io.github.tobia80.dref_consensus.ClusterSnapshot
import io.github.tobia80.raft.KVEntry
import io.github.tobia80.state_command.*
import zio.{Clock, Hub, Task, UIO, ZIO}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

sealed trait ApplyResult
object ApplyResult {
  case object Unit extends ApplyResult
  final case class Created(created: Boolean) extends ApplyResult
  final case class Value(value: Option[Array[Byte]]) extends ApplyResult
}

final class ProtoStateMachine private (changesHub: Hub[ChangeEvent]) {
  private case class ExpiringValue(value: Array[Byte], expireAt: Option[Long])

  private val innerMap = new ConcurrentHashMap[String, ExpiringValue]()

  def apply(cmd: StateCommand): UIO[ApplyResult] =
    cmd.op match {
      case StateCommand.Op.SetElement(SetElementCommand(name, value, expireAt, _)) =>
        val bytes = value.toByteArray
        innerMap.put(name, ExpiringValue(bytes, expireAt))
        changesHub.publish(SetElement(name, bytes)).as(ApplyResult.Unit)

      case StateCommand.Op.SetElementIfNotExist(SetElementIfNotExistCommand(name, value, expireAt, _)) =>
        if innerMap.containsKey(name) then ZIO.succeed(ApplyResult.Created(false))
        else
          val bytes = value.toByteArray
          innerMap.put(name, ExpiringValue(bytes, expireAt))
          changesHub.publish(SetElement(name, bytes)).as(ApplyResult.Created(true))

      case StateCommand.Op.DeleteElement(DeleteElementCommand(name, _)) =>
        innerMap.remove(name)
        changesHub.publish(DeleteElement(name)).as(ApplyResult.Unit)

      case StateCommand.Op.ExpireElement(ExpireElementCommand(name, expireAt, _)) =>
        Option(innerMap.get(name)).foreach { existing =>
          innerMap.put(name, existing.copy(expireAt = Some(expireAt)))
        }
        ZIO.succeed(ApplyResult.Unit)

      case StateCommand.Op.DeleteIfExpired(DeleteIfExpiredCommand(name, now, _)) =>
        val shouldDelete = Option(innerMap.get(name))
          .flatMap(_.expireAt)
          .exists(_ <= now)
        if shouldDelete then
          innerMap.remove(name)
          changesHub.publish(DeleteElement(name)).as(ApplyResult.Unit)
        else ZIO.succeed(ApplyResult.Unit)

      case StateCommand.Op.StartNewTerm(_) | StateCommand.Op.Empty =>
        ZIO.succeed(ApplyResult.Unit)
    }

  def get(name: String): Task[Option[Array[Byte]]] =
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).map { now =>
      Option(innerMap.get(name)).flatMap { v =>
        if v.expireAt.exists(_ <= now) then None else Some(v.value)
      }
    }

  def expirationTable: UIO[Map[String, Long]] =
    ZIO.succeed {
      innerMap.asScala.collect { case (name, ExpiringValue(_, Some(expireAt))) =>
        name -> expireAt
      }.toMap
    }

  def takeSnapshot: UIO[ClusterSnapshot] =
    ZIO.succeed {
      val entries = innerMap.asScala.map { case (key, ExpiringValue(value, expireAt)) =>
        KVEntry(key, ByteString.copyFrom(value), expireAt)
      }.toSeq
      ClusterSnapshot(entries)
    }

  def installSnapshot(snapshot: ClusterSnapshot): UIO[Unit] =
    ZIO.succeed {
      innerMap.clear()
      snapshot.entries.foreach { entry =>
        innerMap.put(entry.key, ExpiringValue(entry.value.toByteArray, entry.expireAt))
      }
    }

  def changeHub: Hub[ChangeEvent] = changesHub
}

object ProtoStateMachine {
  def make: ZIO[Any, Nothing, ProtoStateMachine] =
    Hub.bounded[ChangeEvent](256).map(new ProtoStateMachine(_))
}
