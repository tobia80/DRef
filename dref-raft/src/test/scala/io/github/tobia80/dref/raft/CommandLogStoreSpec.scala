package io.github.tobia80.dref.raft

import io.github.tobia80.dref.raft.proto.{CommandLogEntry, CommandLogState, CommandLogStore, StateCommands}
import zio.*
import zio.test.*

import java.nio.file.{Files, Path}

object CommandLogStoreSpec extends ZIOSpecDefault {

  private def deleteRecursive(path: Path): Unit =
    if java.nio.file.Files.exists(path) then {
      if java.nio.file.Files.isDirectory(path) then {
        val it = java.nio.file.Files.newDirectoryStream(path)
        try it.forEach(deleteRecursive)
        finally it.close()
      }
      java.nio.file.Files.deleteIfExists(path)
      ()
    }

  private val tempDir: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("command-log-spec"))
    )(dir => ZIO.attemptBlocking(deleteRecursive(dir)).orDie)

  private def readGoldenHex(name: String): Task[String] =
    ZIO.attempt {
      val path = java.nio.file.Paths.get(sys.props.getOrElse("user.dir", "."), "compat", "command_log_vectors.json")
      val text = Files.readString(path)
      val pattern = s"""\"name\"\\s*:\\s*\"$name\"[^}]*\"hex\"\\s*:\\s*\"([^\"]*)\"""".r
      pattern.findFirstMatchIn(text).map(_.group(1)).getOrElse("")
    }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("CommandLogStore")(
    test("load on missing file returns empty") {
      for {
        dir   <- tempDir
        store <- CommandLogStore.file(dir)
        state <- store.load
      } yield assertTrue(state == CommandLogState.empty)
    },
    test("append and load round-trip") {
      for {
        dir   <- tempDir
        store <- CommandLogStore.file(dir)
        cmd    = StateCommands.setElement("k", Array[Byte](1, 2), None).toByteArray
        _     <- store.append(1L, cmd)
        _     <- store.setCommitSeq(1L)
        state <- store.load
      } yield assertTrue(
        state.commitSeq == 1L,
        state.entries.get(1L).exists(_.sameElements(cmd))
      )
    },
    test("truncateThrough drops committed prefix") {
      for {
        dir   <- tempDir
        store <- CommandLogStore.file(dir)
        _     <- store.append(1L, Array[Byte](1))
        _     <- store.append(2L, Array[Byte](2))
        _     <- store.setCommitSeq(2L)
        _     <- store.truncateThrough(1L)
        state <- store.load
      } yield assertTrue(
        state.commitSeq == 1L,
        state.entries.get(1L).isEmpty,
        state.entries.get(2L).exists(_.sameElements(Array[Byte](2)))
      )
    },
    test("truncateFrom drops failed suffix and leaves commit untouched") {
      for {
        dir   <- tempDir
        store <- CommandLogStore.file(dir)
        _     <- store.append(1L, Array[Byte](1))
        _     <- store.append(2L, Array[Byte](2))
        _     <- store.append(3L, Array[Byte](3))
        _     <- store.setCommitSeq(2L)
        // Roll back the failed append at seq=3.
        _     <- store.truncateFrom(3L)
        state <- store.load
      } yield assertTrue(
        // commit index is untouched by a rollback.
        state.commitSeq == 2L,
        state.entries.get(1L).exists(_.sameElements(Array[Byte](1))),
        state.entries.get(2L).exists(_.sameElements(Array[Byte](2))),
        state.entries.get(3L).isEmpty
      )
    },
    test("on-disk bytes match cross-language golden vectors") {
      val cmd = StateCommands.setElement("my-key", Array[Byte](1, 2, 3), Some(1700000000L)).toByteArray
      val encoded = CommandLogStore.encodeForTest(CommandLogEntry(1L, cmd), commitSeq = 0L)
      val hex = encoded.map(b => f"$b%02x").mkString
      for {
        expected <- readGoldenHex("single_set_element_record")
      } yield assertTrue(hex == expected, hex.nonEmpty)
    }
  ) @@ TestAspect.sequential
}
