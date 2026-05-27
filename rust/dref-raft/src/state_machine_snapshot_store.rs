//! Persists the Raft state machine's [`ClusterSnapshot`] to disk.
//!
//! The voter state store ([`crate::voter_state_store`]) makes the node Raft-safe across restarts;
//! this store closes the companion gap on the application side. Without it, a restarted node boots
//! with an empty in-memory state and has to be re-seeded by the leader's `InstallSnapshot` RPC.
//! That works, but it adds avoidable network cost and recovery time on every restart.
//!
//! Wire format matches the Scala [`StateMachineSnapshotStore`](../../../dref-raft/src/main/scala/io/github/tobia80/dref/raft/proto/StateMachineSnapshotStore.scala),
//! so a Scala and Rust node can share a storage directory.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use prost::Message;

use crate::proto::dref_consensus::ClusterSnapshot;

const MAGIC: u32 = 0x4452_4653; // "DRFS"
const VERSION: u8 = 1;
const FILE_NAME: &str = "state-snapshot";
const TMP_SUFFIX: &str = ".tmp";

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("state-snapshot file truncated")]
    Truncated,
    #[error("state-snapshot magic mismatch: expected 0x{MAGIC:08x} got 0x{got:08x}")]
    BadMagic { got: u32 },
    #[error("state-snapshot version {version} not supported")]
    BadVersion { version: u8 },
    #[error("state-snapshot payload length negative: {0}")]
    BadLength(i32),
    #[error("state-snapshot protobuf decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Durably records the state machine's `ClusterSnapshot`; lets a restarted node skip the
/// leader's `InstallSnapshot` round when local state is already up-to-date.
pub trait StateMachineSnapshotStore: Send + Sync {
    fn load(&self) -> Result<Option<ClusterSnapshot>, SnapshotError>;
    fn save(&self, snapshot: &ClusterSnapshot) -> Result<(), SnapshotError>;
}

/// In-memory no-op — matches Scala `StateMachineSnapshotStore.noop`.
pub struct NoopStateMachineSnapshotStore;

impl StateMachineSnapshotStore for NoopStateMachineSnapshotStore {
    fn load(&self) -> Result<Option<ClusterSnapshot>, SnapshotError> {
        Ok(None)
    }

    fn save(&self, _snapshot: &ClusterSnapshot) -> Result<(), SnapshotError> {
        Ok(())
    }
}

pub struct FileStateMachineSnapshotStore {
    dir: PathBuf,
}

impl FileStateMachineSnapshotStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, SnapshotError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn target(&self) -> PathBuf {
        self.dir.join(FILE_NAME)
    }

    fn tmp(&self) -> PathBuf {
        self.dir.join(format!("{FILE_NAME}{TMP_SUFFIX}"))
    }
}

impl StateMachineSnapshotStore for FileStateMachineSnapshotStore {
    fn load(&self) -> Result<Option<ClusterSnapshot>, SnapshotError> {
        let path = self.target();
        if !path.exists() {
            return Ok(None);
        }
        let mut file = File::open(&path)?;
        read_snapshot(&mut file).map(Some)
    }

    fn save(&self, snapshot: &ClusterSnapshot) -> Result<(), SnapshotError> {
        let tmp = self.tmp();
        {
            let mut file = File::create(&tmp)?;
            write_snapshot(&mut file, snapshot)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, self.target())?;
        Ok(())
    }
}

fn read_snapshot<R: Read>(mut r: R) -> Result<ClusterSnapshot, SnapshotError> {
    let magic = read_u32_be(&mut r)?;
    if magic != MAGIC {
        return Err(SnapshotError::BadMagic { got: magic });
    }
    let version = read_u8(&mut r)?;
    if version != VERSION {
        return Err(SnapshotError::BadVersion { version });
    }
    let len = read_i32_be(&mut r)?;
    if len < 0 {
        return Err(SnapshotError::BadLength(len));
    }
    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload).map_err(map_eof)?;
    let snapshot = ClusterSnapshot::decode(payload.as_slice())?;
    Ok(snapshot)
}

fn write_snapshot<W: Write>(w: &mut W, snapshot: &ClusterSnapshot) -> Result<(), SnapshotError> {
    let payload = snapshot.encode_to_vec();
    write_u32_be(w, MAGIC)?;
    write_u8(w, VERSION)?;
    write_i32_be(w, payload.len() as i32)?;
    w.write_all(&payload)?;
    Ok(())
}

fn read_u8(r: &mut impl Read) -> Result<u8, SnapshotError> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(b[0])
}

fn read_u32_be(r: &mut impl Read) -> Result<u32, SnapshotError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(u32::from_be_bytes(b))
}

fn read_i32_be(r: &mut impl Read) -> Result<i32, SnapshotError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(i32::from_be_bytes(b))
}

fn write_u8(w: &mut impl Write, v: u8) -> Result<(), SnapshotError> {
    w.write_all(&[v]).map_err(SnapshotError::Io)
}

fn write_u32_be(w: &mut impl Write, v: u32) -> Result<(), SnapshotError> {
    w.write_all(&v.to_be_bytes()).map_err(SnapshotError::Io)
}

fn write_i32_be(w: &mut impl Write, v: i32) -> Result<(), SnapshotError> {
    w.write_all(&v.to_be_bytes()).map_err(SnapshotError::Io)
}

fn map_eof(e: io::Error) -> SnapshotError {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        SnapshotError::Truncated
    } else {
        SnapshotError::Io(e)
    }
}

/// Encode snapshot to bytes (for golden-vector tests).
pub fn encode_for_test(snapshot: &ClusterSnapshot) -> Result<Vec<u8>, SnapshotError> {
    let mut buf = Vec::new();
    write_snapshot(&mut buf, snapshot)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::dref_consensus::KvEntry;
    use std::io::{Seek, SeekFrom, Write};

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dref-snapshot-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn load_missing_returns_none() {
        let dir = temp_dir("missing");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStateMachineSnapshotStore::open(&dir).unwrap();
        assert_eq!(store.load().unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn round_trip_empty() {
        let dir = temp_dir("round-trip-empty");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStateMachineSnapshotStore::open(&dir).unwrap();
        let snap = ClusterSnapshot::default();
        store.save(&snap).unwrap();
        assert_eq!(store.load().unwrap(), Some(snap));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn round_trip_entries() {
        let dir = temp_dir("round-trip-entries");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStateMachineSnapshotStore::open(&dir).unwrap();
        let snap = ClusterSnapshot {
            entries: vec![
                KvEntry {
                    key: "alpha".into(),
                    value: vec![1, 2, 3],
                    expire_at: None,
                },
                KvEntry {
                    key: "beta".into(),
                    value: vec![0x7f, 0x00],
                    expire_at: Some(1_700_000_000_000),
                },
            ],
            last_seq: 17,
        };
        store.save(&snap).unwrap();
        assert_eq!(store.load().unwrap(), Some(snap));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_magic_fails_load() {
        let dir = temp_dir("corrupt");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStateMachineSnapshotStore::open(&dir).unwrap();
        store
            .save(&ClusterSnapshot {
                entries: vec![KvEntry {
                    key: "k".into(),
                    value: vec![1],
                    expire_at: None,
                }],
                last_seq: 0,
            })
            .unwrap();
        let path = store.target();
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&0xdeadbeefu32.to_be_bytes()).unwrap();
        assert!(matches!(store.load(), Err(SnapshotError::BadMagic { .. })));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_file_fails_load() {
        let dir = temp_dir("truncated");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStateMachineSnapshotStore::open(&dir).unwrap();
        store
            .save(&ClusterSnapshot {
                entries: vec![KvEntry {
                    key: "k".into(),
                    value: vec![1, 2, 3],
                    expire_at: None,
                }],
                last_seq: 0,
            })
            .unwrap();
        let path = store.target();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(6).unwrap();
        assert!(matches!(store.load(), Err(SnapshotError::Truncated)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn noop_discards_writes() {
        let store = NoopStateMachineSnapshotStore;
        store
            .save(&ClusterSnapshot {
                entries: vec![KvEntry {
                    key: "ignored".into(),
                    value: vec![9],
                    expire_at: None,
                }],
                last_seq: 0,
            })
            .unwrap();
        assert_eq!(store.load().unwrap(), None);
    }
}
