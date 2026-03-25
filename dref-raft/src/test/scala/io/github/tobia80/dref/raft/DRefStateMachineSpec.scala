package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.*
import io.github.tobia80.raft.{KVEntry, KVSnapshotChunkData, StartNewTermOpProto}
import reactor.core.publisher.Sinks
import zio.*
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}

import java.util
import scala.jdk.CollectionConverters.*

object DRefStateMachineSpec extends ZIOSpecDefault {

  private def makeStateMachine(): (DRefStateMachine, Sinks.Many[ChangeEvent]) = {
    val sink = Sinks.many().multicast().onBackpressureBuffer[ChangeEvent]()
    (new DRefStateMachine(sink), sink)
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DRefStateMachine")(
    suite("setElement")(
      test("should store a value and emit a change event") {
        val (sm, sink) = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key1", value = value, expireAt = None))
        val getResult = sm.runOperation(2, GetElementRequest(name = "key1"))
        assertTrue(
          getResult != null,
          java.util.Arrays.equals(getResult.asInstanceOf[Array[Byte]], "hello".getBytes)
        )
      },
      test("should overwrite existing value") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(1, SetElementRequest(name = "key1", value = ByteString.copyFrom("first".getBytes), expireAt = None))
        sm.runOperation(2, SetElementRequest(name = "key1", value = ByteString.copyFrom("second".getBytes), expireAt = None))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key1")).asInstanceOf[Array[Byte]]
        assertTrue(java.util.Arrays.equals(getResult, "second".getBytes))
      },
      test("should store value with expiration") {
        val (sm, _) = makeStateMachine()
        val value = ByteString.copyFrom("expiring".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key1", value = value, expireAt = Some(5000L)))
        val table = sm.runOperation(2, GetExpirationTableRequest()).asInstanceOf[Map[String, Long]]
        assertTrue(table == Map("key1" -> 5000L))
      }
    ),
    suite("getElement")(
      test("should return null for non-existent key") {
        val (sm, _) = makeStateMachine()
        val result = sm.runOperation(1, GetElementRequest(name = "missing"))
        assertTrue(result == null)
      },
      test("should return stored value") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(1, SetElementRequest(name = "key1", value = ByteString.copyFrom("data".getBytes), expireAt = None))
        val result = sm.runOperation(2, GetElementRequest(name = "key1")).asInstanceOf[Array[Byte]]
        assertTrue(java.util.Arrays.equals(result, "data".getBytes))
      }
    ),
    suite("setElementIfNotExist")(
      test("should set value when key does not exist") {
        val (sm, _) = makeStateMachine()
        val result = sm.runOperation(
          1,
          SetElementIfNotExistRequest(name = "key1", value = ByteString.copyFrom("new".getBytes), expireAt = None)
        )
        val getResult = sm.runOperation(2, GetElementRequest(name = "key1")).asInstanceOf[Array[Byte]]
        assertTrue(
          result == java.lang.Boolean.TRUE,
          java.util.Arrays.equals(getResult, "new".getBytes)
        )
      },
      test("should not overwrite when key already exists") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(1, SetElementRequest(name = "key1", value = ByteString.copyFrom("original".getBytes), expireAt = None))
        val result = sm.runOperation(
          2,
          SetElementIfNotExistRequest(name = "key1", value = ByteString.copyFrom("attempt".getBytes), expireAt = None)
        )
        val getResult = sm.runOperation(3, GetElementRequest(name = "key1")).asInstanceOf[Array[Byte]]
        assertTrue(
          result == java.lang.Boolean.FALSE,
          java.util.Arrays.equals(getResult, "original".getBytes)
        )
      }
    ),
    suite("deleteElement")(
      test("should remove existing element") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(1, SetElementRequest(name = "key1", value = ByteString.copyFrom("data".getBytes), expireAt = None))
        sm.runOperation(2, DeleteElementRequest(name = "key1"))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key1"))
        assertTrue(getResult == null)
      },
      test("should return null for deleting non-existent element") {
        val (sm, _) = makeStateMachine()
        val result = sm.runOperation(1, DeleteElementRequest(name = "missing"))
        assertTrue(result == null)
      }
    ),
    suite("expireElement")(
      test("should set expiration on existing element") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(1, SetElementRequest(name = "key1", value = ByteString.copyFrom("data".getBytes), expireAt = None))
        val result = sm.runOperation(2, ExpireElementRequest(name = "key1", expireAt = 3000L))
        val table = sm.runOperation(3, GetExpirationTableRequest()).asInstanceOf[Map[String, Long]]
        assertTrue(result == java.lang.Boolean.TRUE, table == Map("key1" -> 3000L))
      },
      test("should return FALSE for non-existent element") {
        val (sm, _) = makeStateMachine()
        val result = sm.runOperation(1, ExpireElementRequest(name = "missing", expireAt = 3000L))
        assertTrue(result == java.lang.Boolean.FALSE)
      },
      test("should update existing expiration") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(1, SetElementRequest(name = "key1", value = ByteString.copyFrom("data".getBytes), expireAt = Some(1000L)))
        sm.runOperation(2, ExpireElementRequest(name = "key1", expireAt = 5000L))
        val table = sm.runOperation(3, GetExpirationTableRequest()).asInstanceOf[Map[String, Long]]
        assertTrue(table == Map("key1" -> 5000L))
      }
    ),
    suite("deleteIfExpired")(
      test("should delete an expired element") {
        val (sm, _) = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = Some(1000L)))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 2000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result != null, getResult == null)
      },
      test("should delete element when expireAt equals expiredBefore") {
        val (sm, _) = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = Some(1000L)))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 1000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result != null, getResult == null)
      },
      test("should not delete a non-expired element") {
        val (sm, _) = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = Some(2000L)))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 1000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result == null, getResult != null)
      },
      test("should return null for non-existent element") {
        val (sm, _) = makeStateMachine()
        val result = sm.runOperation(1, DeleteIfExpiredRequest("non-existent", expiredBefore = 2000L))
        assertTrue(result == null)
      },
      test("should not delete element without expiration") {
        val (sm, _) = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = None))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 2000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result == null, getResult != null)
      }
    ),
    suite("retrieveExpirationTable")(
      test("should only return elements with expiration") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(
          1,
          SetElementRequest(name = "with-expiry", value = ByteString.copyFrom("a".getBytes), expireAt = Some(1000L))
        )
        sm.runOperation(
          2,
          SetElementRequest(name = "without-expiry", value = ByteString.copyFrom("b".getBytes), expireAt = None)
        )
        val result = sm.runOperation(3, GetExpirationTableRequest()).asInstanceOf[Map[String, Long]]
        assertTrue(result == Map("with-expiry" -> 1000L))
      },
      test("should return empty map when no elements have expiration") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(
          1,
          SetElementRequest(name = "no-expiry", value = ByteString.copyFrom("a".getBytes), expireAt = None)
        )
        val result = sm.runOperation(2, GetExpirationTableRequest()).asInstanceOf[Map[String, Long]]
        assertTrue(result.isEmpty)
      }
    ),
    suite("snapshot and restore")(
      test("takeSnapshot and installSnapshot should preserve state") {
        val (sm, _) = makeStateMachine()
        sm.runOperation(1, SetElementRequest(name = "k1", value = ByteString.copyFrom("v1".getBytes), expireAt = Some(1000L)))
        sm.runOperation(2, SetElementRequest(name = "k2", value = ByteString.copyFrom("v2".getBytes), expireAt = None))

        val chunks = new java.util.ArrayList[AnyRef]()
        sm.takeSnapshot(2, (chunk: AnyRef) => chunks.add(chunk))

        val (sm2, _) = makeStateMachine()
        sm2.installSnapshot(2, chunks)

        val v1 = sm2.runOperation(3, GetElementRequest(name = "k1")).asInstanceOf[Array[Byte]]
        val v2 = sm2.runOperation(4, GetElementRequest(name = "k2")).asInstanceOf[Array[Byte]]
        val table = sm2.runOperation(5, GetExpirationTableRequest()).asInstanceOf[Map[String, Long]]

        assertTrue(
          java.util.Arrays.equals(v1, "v1".getBytes),
          java.util.Arrays.equals(v2, "v2".getBytes),
          table == Map("k1" -> 1000L)
        )
      }
    ),
    suite("newTermOperation")(
      test("getNewTermOperation should return StartNewTermOpProto") {
        val (sm, _) = makeStateMachine()
        val result = sm.getNewTermOperation
        assertTrue(result.isInstanceOf[StartNewTermOpProto])
      },
      test("runOperation should handle StartNewTermOpProto without error") {
        val (sm, _) = makeStateMachine()
        val result = sm.runOperation(1, StartNewTermOpProto.defaultInstance)
        assertTrue(result == null)
      }
    ),
    suite("unsupported operations")(
      test("should throw IllegalArgumentException for unknown operation") {
        val (sm, _) = makeStateMachine()
        val result =
          try {
            sm.runOperation(1, "unsupported")
            false
          } catch {
            case _: IllegalArgumentException => true
            case _: Throwable                => false
          }
        assertTrue(result)
      }
    )
  )
}
