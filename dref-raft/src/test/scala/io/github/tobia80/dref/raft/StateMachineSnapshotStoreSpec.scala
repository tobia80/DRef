package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.raft.proto.StateMachineSnapshotStore
import io.github.tobia80.dref_consensus.ClusterSnapshot
import io.github.tobia80.raft.KVEntry
import zio.*
import zio.test.*

import java.io.{IOException, RandomAccessFile}
import java.nio.file.{Files, Path}

object StateMachineSnapshotStoreSpec extends ZIOSpecDefault {

  private val tempDir: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("snapshot-store-spec"))
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

  private def kv(key: String, value: Array[Byte], expireAt: Option[Long] = None): KVEntry =
    KVEntry(key = key, value = ByteString.copyFrom(value), expireAt = expireAt)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StateMachineSnapshotStore")(
    test("load on missing file returns None") {
      for {
        dir   <- tempDir
        store <- StateMachineSnapshotStore.file(dir)
        state <- store.load
      } yield assertTrue(state.isEmpty)
    },
    test("save then load round-trips an empty snapshot") {
      for {
        dir    <- tempDir
        store  <- StateMachineSnapshotStore.file(dir)
        _      <- store.save(ClusterSnapshot())
        loaded <- store.load
      } yield assertTrue(loaded.contains(ClusterSnapshot()))
    },
    test("save then load round-trips entries (bytes, key, expireAt)") {
      val snapshot = ClusterSnapshot(
        entries = Seq(
          kv("alpha", Array[Byte](1, 2, 3)),
          kv("beta", Array[Byte](0x7f, 0x00), Some(1700000000000L)),
          kv("gamma", Array.emptyByteArray)
        ),
        lastSeq = 17L
      )
      for {
        dir    <- tempDir
        store  <- StateMachineSnapshotStore.file(dir)
        _      <- store.save(snapshot)
        loaded <- store.load
      } yield assertTrue(loaded.contains(snapshot))
    },
    test("overwrite: later save wins") {
      val first = ClusterSnapshot(entries = Seq(kv("first", Array[Byte](1))))
      val second = ClusterSnapshot(entries = Seq(kv("second", Array[Byte](2))))
      for {
        dir    <- tempDir
        store  <- StateMachineSnapshotStore.file(dir)
        _      <- store.save(first)
        _      <- store.save(second)
        loaded <- store.load
      } yield assertTrue(loaded.contains(second))
    },
    test("a new store instance over an existing dir reads previous snapshot") {
      val snapshot = ClusterSnapshot(entries = Seq(kv("survivor", Array[Byte](42))))
      for {
        dir    <- tempDir
        first  <- StateMachineSnapshotStore.file(dir)
        _      <- first.save(snapshot)
        second <- StateMachineSnapshotStore.file(dir)
        loaded <- second.load
      } yield assertTrue(loaded.contains(snapshot))
    },
    test("corrupt magic header fails load") {
      for {
        dir    <- tempDir
        store  <- StateMachineSnapshotStore.file(dir)
        _      <- store.save(ClusterSnapshot(entries = Seq(kv("k", Array[Byte](1)))))
        _      <- ZIO.attemptBlocking {
                    val f = new RandomAccessFile(dir.resolve("state-snapshot").toFile, "rw")
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
        dir    <- tempDir
        store  <- StateMachineSnapshotStore.file(dir)
        _      <- store.save(ClusterSnapshot(entries = Seq(kv("k", Array[Byte](1, 2, 3)))))
        _      <- ZIO.attemptBlocking {
                    val f = new RandomAccessFile(dir.resolve("state-snapshot").toFile, "rw")
                    try f.setLength(6)
                    finally f.close()
                  }
        result <- store.load.exit
      } yield assertTrue(result.isFailure)
    },
    test("noop store discards writes and always loads None") {
      val store = StateMachineSnapshotStore.noop
      for {
        _     <- store.save(ClusterSnapshot(entries = Seq(kv("ignored", Array[Byte](9)))))
        state <- store.load
      } yield assertTrue(state.isEmpty)
    },
    test("on-disk bytes match cross-language golden vectors") {
      // Empty snapshot: magic DRFS + version 1 + length 0 (no protobuf payload).
      // Single entry (key="a", value=0x01, no expireAt): the protobuf payload is
      // ClusterSnapshot { entries: [KVEntry { key:"a", value:[0x01] }] }
      //   = field 1 (length-delimited) tag 0x0a, length 6, then KVEntry bytes
      //   KVEntry = 0a 01 61 12 01 01   (string "a", bytes 0x01)
      //   Outer  = 0a 06 0a 01 61 12 01 01  (8 bytes)
      for {
        dir       <- tempDir
        store     <- StateMachineSnapshotStore.file(dir)
        _         <- store.save(ClusterSnapshot())
        emptyHex  <- ZIO.attemptBlocking(Files.readAllBytes(dir.resolve("state-snapshot"))).map(hex)
        _         <- store.save(ClusterSnapshot(entries = Seq(kv("a", Array[Byte](0x01)))))
        singleHex <- ZIO.attemptBlocking(Files.readAllBytes(dir.resolve("state-snapshot"))).map(hex)
        _         <- store.save(ClusterSnapshot(entries = Seq(kv("a", Array[Byte](0x01))), lastSeq = 42L))
        seqHex    <- ZIO.attemptBlocking(Files.readAllBytes(dir.resolve("state-snapshot"))).map(hex)
      } yield assertTrue(
        emptyHex == "445246530100000000",
        singleHex == "4452465301000000080a060a0161120101",
        seqHex == "44524653010000000a0a060a0161120101102a"
      )
    }
  ) @@ TestAspect.sequential

  private def hex(bytes: Array[Byte]): String =
    bytes.map(b => f"$b%02x").mkString
}
