//! Persists Raft voter state (`currentTerm`, `votedFor`) across restarts.
//!
//! Wire format matches the Scala [`VoterStateStore`](../../dref-raft/src/main/scala/io/github/tobia80/dref/raft/proto/VoterStateStore.scala)
//! so nodes can share a storage directory across languages.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x4452_4654; // "DRFT"
const VERSION: u8 = 1;
const FILE_NAME: &str = "voter-state";
const TMP_SUFFIX: &str = ".tmp";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoterState {
    pub term: u64,
    pub voted_for: Option<String>,
}

impl VoterState {
    pub const EMPTY: Self = Self {
        term: 0,
        voted_for: None,
    };
}

#[derive(Debug, thiserror::Error)]
pub enum VoterStateError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("voter-state file truncated")]
    Truncated,
    #[error("voter-state magic mismatch: expected 0x{MAGIC:08x} got 0x{got:08x}")]
    BadMagic { got: u32 },
    #[error("voter-state version {version} not supported")]
    BadVersion { version: u8 },
}

/// Durably records term and vote; required for safe restart in multi-node clusters.
pub trait VoterStateStore: Send + Sync {
    fn load(&self) -> Result<VoterState, VoterStateError>;
    fn save(&self, state: &VoterState) -> Result<(), VoterStateError>;
}

/// In-memory no-op — matches Scala `VoterStateStore.noop`.
pub struct NoopVoterStateStore;

impl VoterStateStore for NoopVoterStateStore {
    fn load(&self) -> Result<VoterState, VoterStateError> {
        Ok(VoterState::EMPTY)
    }

    fn save(&self, _state: &VoterState) -> Result<(), VoterStateError> {
        Ok(())
    }
}

pub struct FileVoterStateStore {
    dir: PathBuf,
}

impl FileVoterStateStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, VoterStateError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn target(&self) -> PathBuf {
        self.dir.join(FILE_NAME)
    }

    fn tmp(&self) -> PathBuf {
        self.dir.join(format!("{FILE_NAME}{TMP_SUFFIX}"))
    }
}

impl VoterStateStore for FileVoterStateStore {
    fn load(&self) -> Result<VoterState, VoterStateError> {
        let path = self.target();
        if !path.exists() {
            return Ok(VoterState::EMPTY);
        }
        let mut file = File::open(&path)?;
        read_state(&mut file)
    }

    fn save(&self, state: &VoterState) -> Result<(), VoterStateError> {
        let tmp = self.tmp();
        {
            let mut file = File::create(&tmp)?;
            write_state(&mut file, state)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, self.target())?;
        Ok(())
    }
}

fn read_state<R: Read>(mut r: R) -> Result<VoterState, VoterStateError> {
    let magic = read_u32_be(&mut r)?;
    if magic != MAGIC {
        return Err(VoterStateError::BadMagic { got: magic });
    }
    let version = read_u8(&mut r)?;
    if version != VERSION {
        return Err(VoterStateError::BadVersion { version });
    }
    let term = read_u64_be(&mut r)?;
    let len = read_i32_be(&mut r)?;
    let voted_for = if len < 0 {
        None
    } else if len == 0 {
        Some(String::new())
    } else {
        let len = len as usize;
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                VoterStateError::Truncated
            } else {
                VoterStateError::Io(e)
            }
        })?;
        Some(
            String::from_utf8(buf)
                .map_err(|e| VoterStateError::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?,
        )
    };
    Ok(VoterState { term, voted_for })
}

fn write_state<W: Write>(w: &mut W, state: &VoterState) -> Result<(), VoterStateError> {
    write_u32_be(w, MAGIC)?;
    write_u8(w, VERSION)?;
    write_u64_be(w, state.term)?;
    match &state.voted_for {
        None => write_i32_be(w, -1)?,
        Some(id) => {
            let bytes = id.as_bytes();
            write_i32_be(w, bytes.len() as i32)?;
            w.write_all(bytes)?;
        }
    }
    Ok(())
}

fn read_u8(r: &mut impl Read) -> Result<u8, VoterStateError> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(b[0])
}

fn read_u32_be(r: &mut impl Read) -> Result<u32, VoterStateError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(u32::from_be_bytes(b))
}

fn read_i32_be(r: &mut impl Read) -> Result<i32, VoterStateError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(i32::from_be_bytes(b))
}

fn read_u64_be(r: &mut impl Read) -> Result<u64, VoterStateError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(u64::from_be_bytes(b))
}

fn write_u8(w: &mut impl Write, v: u8) -> Result<(), VoterStateError> {
    w.write_all(&[v]).map_err(VoterStateError::Io)
}

fn write_u32_be(w: &mut impl Write, v: u32) -> Result<(), VoterStateError> {
    w.write_all(&v.to_be_bytes()).map_err(VoterStateError::Io)
}

fn write_i32_be(w: &mut impl Write, v: i32) -> Result<(), VoterStateError> {
    w.write_all(&v.to_be_bytes()).map_err(VoterStateError::Io)
}

fn write_u64_be(w: &mut impl Write, v: u64) -> Result<(), VoterStateError> {
    w.write_all(&v.to_be_bytes()).map_err(VoterStateError::Io)
}

fn map_eof(e: io::Error) -> VoterStateError {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        VoterStateError::Truncated
    } else {
        VoterStateError::Io(e)
    }
}

/// Encode voter state to bytes (for golden-vector tests).
pub fn encode_for_test(state: &VoterState) -> Result<Vec<u8>, VoterStateError> {
    let mut buf = Vec::new();
    write_state(&mut buf, state)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dref-voter-state-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn load_missing_returns_empty() {
        let dir = temp_dir("missing");
        let _ = fs::remove_dir_all(&dir);
        let store = FileVoterStateStore::open(&dir).unwrap();
        assert_eq!(store.load().unwrap(), VoterState::EMPTY);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn round_trip_no_vote() {
        let dir = temp_dir("round-trip-none");
        let _ = fs::remove_dir_all(&dir);
        let store = FileVoterStateStore::open(&dir).unwrap();
        store
            .save(&VoterState {
                term: 7,
                voted_for: None,
            })
            .unwrap();
        assert_eq!(
            store.load().unwrap(),
            VoterState {
                term: 7,
                voted_for: None
            }
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn round_trip_with_vote() {
        let dir = temp_dir("round-trip-vote");
        let _ = fs::remove_dir_all(&dir);
        let store = FileVoterStateStore::open(&dir).unwrap();
        store
            .save(&VoterState {
                term: 42,
                voted_for: Some("node-7".to_string()),
            })
            .unwrap();
        assert_eq!(
            store.load().unwrap(),
            VoterState {
                term: 42,
                voted_for: Some("node-7".to_string())
            }
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_magic_fails_load() {
        let dir = temp_dir("corrupt");
        let _ = fs::remove_dir_all(&dir);
        let store = FileVoterStateStore::open(&dir).unwrap();
        store
            .save(&VoterState {
                term: 5,
                voted_for: Some("n".into()),
            })
            .unwrap();
        let path = store.target();
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&0xdeadbeefu32.to_be_bytes()).unwrap();
        assert!(matches!(
            store.load(),
            Err(VoterStateError::BadMagic { .. })
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_file_fails_load() {
        let dir = temp_dir("truncated");
        let _ = fs::remove_dir_all(&dir);
        let store = FileVoterStateStore::open(&dir).unwrap();
        store
            .save(&VoterState {
                term: 8,
                voted_for: Some("x".into()),
            })
            .unwrap();
        let path = store.target();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(6).unwrap();
        assert!(matches!(store.load(), Err(VoterStateError::Truncated)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn noop_discards_writes() {
        let store = NoopVoterStateStore;
        store
            .save(&VoterState {
                term: 99,
                voted_for: Some("ignored".into()),
            })
            .unwrap();
        assert_eq!(store.load().unwrap(), VoterState::EMPTY);
    }
}
