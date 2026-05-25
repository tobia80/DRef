package io.github.tobia80.dref.raft

import io.github.tobia80.dref.raft.proto.{VoterState, VoterStateStore}
import zio.*
import zio.test.*

import java.io.DataOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** On-disk voter-state bytes shared with Rust `crosslang_voter_state_tests.rs`. */
object CrossLangVoterStateSpec extends ZIOSpecDefault {

  private def hex(bytes: Array[Byte]): String =
    bytes.map(b => f"$b%02x").mkString

  private def deleteRecursive(path: java.nio.file.Path): Unit =
    if Files.exists(path) then {
      if Files.isDirectory(path) then {
        val it = Files.newDirectoryStream(path)
        try it.forEach(deleteRecursive)
        finally it.close()
      }
      Files.deleteIfExists(path)
      ()
    }

  private def writeGolden(state: VoterState): Array[Byte] = {
    val buf = new java.io.ByteArrayOutputStream()
    val out = new DataOutputStream(buf)
    out.writeInt(0x44524654) // DRFT — must match VoterStateStore private Magic
    out.writeByte(1)
    out.writeLong(state.term)
    state.votedFor match {
      case None     => out.writeInt(-1)
      case Some(id) =>
        val bytes = id.getBytes(StandardCharsets.UTF_8)
        out.writeInt(bytes.length)
        out.write(bytes)
    }
    out.flush()
    buf.toByteArray
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("Cross-language voter-state file format")(
    test("term 7 with no vote matches golden bytes") {
      ZIO.succeed(
        assertTrue(hex(writeGolden(VoterState(7L, None))) == Term7NoVoteHex)
      )
    },
    test("term 42 voted for node-7 matches golden bytes") {
      ZIO.succeed(
        assertTrue(hex(writeGolden(VoterState(42L, Some("node-7")))) == Term42Node7Hex)
      )
    },
    test("file store round-trip matches in-memory golden encoding") {
      for {
        dir   <- ZIO.acquireRelease(
                   ZIO.attemptBlocking(Files.createTempDirectory("crosslang-voter"))
                 )(d => ZIO.attemptBlocking(deleteRecursive(d)).orDie)
        store <- VoterStateStore.file(dir)
        state  = VoterState(42L, Some("node-7"))
        _     <- store.save(state)
        loaded <- store.load
        onDisk = Files.readAllBytes(dir.resolve("voter-state"))
      } yield assertTrue(
        loaded == state,
        hex(onDisk) == Term42Node7Hex
      )
    }
  )

  private val Term7NoVoteHex =
    "44524654010000000000000007ffffffff"
  private val Term42Node7Hex =
    "4452465401000000000000002a000000066e6f64652d37"
}
