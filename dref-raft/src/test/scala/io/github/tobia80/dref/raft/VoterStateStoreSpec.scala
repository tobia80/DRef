package io.github.tobia80.dref.raft

import io.github.tobia80.dref.raft.proto.{VoterState, VoterStateStore}
import zio.*
import zio.test.*

import java.io.{IOException, RandomAccessFile}
import java.nio.file.{Files, Path}

object VoterStateStoreSpec extends ZIOSpecDefault {

  private val tempDir: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("voter-state-spec"))
    )(dir => ZIO.attemptBlocking(deleteRecursive(dir)).orDie)

  private def deleteRecursive(path: Path): Unit =
    if Files.exists(path) then {
      if Files.isDirectory(path) then {
        val it = Files.newDirectoryStream(path)
        try it.forEach(deleteRecursive)
        finally it.close()
      }
      Files.deleteIfExists(path)
      ()
    }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("VoterStateStore")(
    test("load on missing file returns empty state") {
      for {
        dir   <- tempDir
        store <- VoterStateStore.file(dir)
        state <- store.load
      } yield assertTrue(state == VoterState.empty)
    },
    test("save then load round-trips term with no vote") {
      for {
        dir    <- tempDir
        store  <- VoterStateStore.file(dir)
        _      <- store.save(VoterState(7L, None))
        loaded <- store.load
      } yield assertTrue(loaded == VoterState(7L, None))
    },
    test("save then load round-trips term and votedFor") {
      for {
        dir    <- tempDir
        store  <- VoterStateStore.file(dir)
        _      <- store.save(VoterState(42L, Some("node-7")))
        loaded <- store.load
      } yield assertTrue(loaded == VoterState(42L, Some("node-7")))
    },
    test("overwrite: later save wins") {
      for {
        dir    <- tempDir
        store  <- VoterStateStore.file(dir)
        _      <- store.save(VoterState(1L, Some("a")))
        _      <- store.save(VoterState(2L, Some("b")))
        _      <- store.save(VoterState(3L, None))
        loaded <- store.load
      } yield assertTrue(loaded == VoterState(3L, None))
    },
    test("two stores in different dirs are independent") {
      for {
        dirA  <- tempDir
        dirB  <- tempDir
        a     <- VoterStateStore.file(dirA)
        b     <- VoterStateStore.file(dirB)
        _     <- a.save(VoterState(10L, Some("alpha")))
        _     <- b.save(VoterState(20L, Some("beta")))
        readA <- a.load
        readB <- b.load
      } yield assertTrue(
        readA == VoterState(10L, Some("alpha")),
        readB == VoterState(20L, Some("beta"))
      )
    },
    test("a new store instance over an existing dir reads previous state") {
      for {
        dir    <- tempDir
        first  <- VoterStateStore.file(dir)
        _      <- first.save(VoterState(99L, Some("survivor")))
        second <- VoterStateStore.file(dir)
        loaded <- second.load
      } yield assertTrue(loaded == VoterState(99L, Some("survivor")))
    },
    test("corrupt magic header fails load") {
      for {
        dir   <- tempDir
        store <- VoterStateStore.file(dir)
        _     <- store.save(VoterState(5L, Some("node-1")))
        _     <- ZIO.attemptBlocking {
                   val f = new RandomAccessFile(dir.resolve("voter-state").toFile, "rw")
                   try {
                     f.seek(0)
                     f.writeInt(0xdeadbeef)
                   } finally f.close()
                 }
        result <- store.load.exit
      } yield assertTrue(
        result.isFailure,
        result.causeOption.exists(_.failureOption.exists(_.isInstanceOf[IOException]))
      )
    },
    test("truncated file fails load") {
      for {
        dir   <- tempDir
        store <- VoterStateStore.file(dir)
        _     <- store.save(VoterState(8L, Some("node-trunc")))
        _     <- ZIO.attemptBlocking {
                   val f = new RandomAccessFile(dir.resolve("voter-state").toFile, "rw")
                   try f.setLength(6)
                   finally f.close()
                 }
        result <- store.load.exit
      } yield assertTrue(result.isFailure)
    },
    test("noop store discards writes and always loads empty") {
      val store = VoterStateStore.noop
      for {
        _     <- store.save(VoterState(123L, Some("ignored")))
        state <- store.load
      } yield assertTrue(state == VoterState.empty)
    }
  ) @@ TestAspect.sequential
}
