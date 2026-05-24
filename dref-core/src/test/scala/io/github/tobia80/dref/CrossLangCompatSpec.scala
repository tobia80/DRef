package io.github.tobia80.dref

import io.github.tobia80.dref.DRef.msgpack.*
import io.github.tobia80.dref.DRefCodec
import zio.*
import zio.schema.{DeriveSchema, Schema}
import zio.test.*

/** Wire-format tests shared with the Rust port. Both sides must agree on these
  * golden bytes for multi-language Redis/Raft clients to interoperate.
  */
object CrossLangCompatSpec extends QuietZIOSpec {

  case class Wrapper(value: String)
  given Schema[Wrapper] = DeriveSchema.gen[Wrapper]

  case class ChangePayload(name: Chunk[Byte], value: Chunk[Byte], delete: Boolean = false)
  given Schema[ChangePayload] = DeriveSchema.gen[ChangePayload]
  given DRefCodec[ChangePayload] = derived[ChangePayload]

  private def hex(bytes: Array[Byte]): String =
    bytes.map(b => f"$b%02x").mkString

  override def spec: Spec[Any, Any] = suite("Cross-language wire format")(
    test("lock value uses 8-byte big-endian Long") {
      val bytes = LockValue.toBytes(42L)
      ZIO.succeed(
        assertTrue(
          bytes.length == 8,
          hex(bytes) == "000000000000002a",
          LockValue.fromBytes(bytes) == 42L
        )
      )
    },
    test("ChangePayload set notification matches golden bytes") {
      for {
        encoded <- DRefCodec.serializeToArray(
                     ChangePayload(
                       Chunk.fromArray("my-key".getBytes),
                       Chunk.fromArray(Array[Byte](1, 2, 3)),
                       delete = false
                     )
                   )
      } yield assertTrue(hex(encoded) == ChangePayloadSetHex)
    },
    test("ChangePayload delete notification matches golden bytes") {
      for {
        encoded <- DRefCodec.serializeToArray(
                     ChangePayload(
                       Chunk.fromArray("my-key".getBytes),
                       Chunk.empty,
                       delete = true
                     )
                   )
      } yield assertTrue(hex(encoded) == ChangePayloadDeleteHex)
    },
    test("MsgPack string value matches golden bytes") {
      for {
        encoded <- DRefCodec.serializeToArray("hello")
      } yield assertTrue(hex(encoded) == StringHelloHex)
    },
    test("MsgPack Wrapper struct matches golden bytes") {
      for {
        encoded <- DRefCodec.serializeToArray(Wrapper("hello"))
      } yield assertTrue(hex(encoded) == WrapperHelloHex)
    }
  )

  // Golden bytes verified against Rust `compat_tests.rs` (rmp-serde named maps).
  private val ChangePayloadSetHex =
    "83a46e616d65966d792d6b6579a576616c756593010203a664656c657465c2"
  private val ChangePayloadDeleteHex =
    "83a46e616d65966d792d6b6579a576616c756590a664656c657465c3"
  private val StringHelloHex = "a568656c6c6f"
  private val WrapperHelloHex = "81a576616c7565a568656c6c6f"
}
