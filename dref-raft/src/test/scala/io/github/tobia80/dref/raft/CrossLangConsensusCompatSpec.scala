package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref_consensus.ClusterSnapshot
import io.github.tobia80.raft.KVEntry
import io.github.tobia80.dref.raft.proto.StateCommands
import zio.*
import zio.test.*

import java.nio.file.{Files, Paths}

/** Golden protobuf bytes shared with the Rust port (prost). */
object CrossLangConsensusCompatSpec extends ZIOSpecDefault {

  private def hex(bytes: Array[Byte]): String =
    bytes.map(b => f"$b%02x").mkString

  private def readPayloadHex(section: String): Task[String] =
    ZIO.attempt {
      val path = Paths.get(sys.props.getOrElse("user.dir", "."), "compat", "consensus_vectors.json")
      val text = Files.readString(path)
      val pattern = s"""\"$section\"[^}]*\"payload_hex\"\\s*:\\s*\"([^\"]*)\"""".r
      pattern.findFirstMatchIn(text).map(_.group(1)).getOrElse("PLACEHOLDER")
    }

  override def spec: Spec[TestEnvironment, Any] = suite("Cross-language consensus protobuf")(
    test("StateCommand set_element matches golden bytes") {
      val cmd = StateCommands.setElement("my-key", Array[Byte](1, 2, 3), Some(1700000000L))
      val payloadHex = hex(cmd.toByteArray)
      for {
        expected <- readPayloadHex("state_command_set_element")
      } yield assertTrue(payloadHex == expected, payloadHex.nonEmpty)
    },
    test("StateCommand delete_element matches golden bytes") {
      val cmd = StateCommands.deleteElement("my-key")
      val payloadHex = hex(cmd.toByteArray)
      for {
        expected <- readPayloadHex("state_command_delete_element")
      } yield assertTrue(payloadHex == expected, payloadHex.nonEmpty)
    },
    test("ClusterSnapshot single entry matches golden bytes") {
      val snapshot = ClusterSnapshot(
        Seq(KVEntry("shared-key", ByteString.copyFrom(Array[Byte](0x68, 0x69)), Some(1700000000L)))
      )
      val payloadHex = hex(snapshot.toByteArray)
      for {
        expected <- readPayloadHex("cluster_snapshot_single_entry")
      } yield assertTrue(payloadHex == expected, payloadHex.nonEmpty)
    },
    test("print set_element hex for compat/consensus_vectors.json") {
      val cmd = StateCommands.setElement("my-key", Array[Byte](1, 2, 3), Some(1700000000L))
      val payloadHex = hex(cmd.toByteArray)
      ZIO.logInfo(s"state_command_set_element payload_hex = $payloadHex") *>
        ZIO.succeed(assertTrue(payloadHex.length >= 10))
    }
  )
}
