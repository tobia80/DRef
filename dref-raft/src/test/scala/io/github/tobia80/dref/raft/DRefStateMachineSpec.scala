package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.*
import reactor.core.publisher.Sinks
import zio.*
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}

object DRefStateMachineSpec extends ZIOSpecDefault {

  private def makeStateMachine(): DRefStateMachine = {
    val sink = Sinks.many().multicast().onBackpressureBuffer[ChangeEvent]()
    new DRefStateMachine(sink)
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DRefStateMachine")(
    suite("deleteIfExpired")(
      test("should delete an expired element") {
        val sm = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = Some(1000L)))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 2000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result != null, getResult == null)
      },
      test("should delete element when expireAt equals expiredBefore") {
        val sm = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = Some(1000L)))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 1000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result != null, getResult == null)
      },
      test("should not delete a non-expired element") {
        val sm = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = Some(2000L)))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 1000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result == null, getResult != null)
      },
      test("should return null for non-existent element") {
        val sm = makeStateMachine()
        val result = sm.runOperation(1, DeleteIfExpiredRequest("non-existent", expiredBefore = 2000L))
        assertTrue(result == null)
      },
      test("should not delete element without expiration") {
        val sm = makeStateMachine()
        val value = ByteString.copyFrom("hello".getBytes)
        sm.runOperation(1, SetElementRequest(name = "key", value = value, expireAt = None))
        val result = sm.runOperation(2, DeleteIfExpiredRequest("key", expiredBefore = 2000L))
        val getResult = sm.runOperation(3, GetElementRequest(name = "key"))
        assertTrue(result == null, getResult != null)
      }
    ),
    suite("retrieveExpirationTable")(
      test("should only return elements with expiration") {
        val sm = makeStateMachine()
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
        val sm = makeStateMachine()
        sm.runOperation(
          1,
          SetElementRequest(name = "no-expiry", value = ByteString.copyFrom("a".getBytes), expireAt = None)
        )
        val result = sm.runOperation(2, GetExpirationTableRequest()).asInstanceOf[Map[String, Long]]
        assertTrue(result.isEmpty)
      }
    )
  )
}
