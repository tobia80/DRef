//! Append-only command log persisted before replication acks.
//!
//! Wire format matches Scala [`CommandLogStore`](../../dref-raft/src/main/scala/io/github/tobia80/dref/raft/proto/CommandLogStore.scala).

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x4452_464c; // "DRFL"
const VERSION: u8 = 1;
const FILE_NAME: &str = "command-log";
const TMP_SUFFIX: &str = ".tmp";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLogEntry {
    pub seq: u64,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLogState {
    pub commit_seq: u64,
    pub entries: BTreeMap<u64, Vec<u8>>,
}

impl CommandLogState {
    pub fn empty() -> Self {
        Self {
            commit_seq: 0,
            entries: BTreeMap::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CommandLogError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("command-log file truncated")]
    Truncated,
    #[error("command-log magic mismatch: expected 0x{MAGIC:08x} got 0x{got:08x}")]
    BadMagic { got: u32 },
    #[error("command-log version {version} not supported")]
    BadVersion { version: u8 },
    #[error("command-log negative command length at seq {seq}")]
    NegativeLength { seq: u64 },
}

pub trait CommandLogStore: Send + Sync {
    fn load(&self) -> Result<CommandLogState, CommandLogError>;
    fn append(&self, seq: u64, command: &[u8]) -> Result<(), CommandLogError>;
    fn set_commit_seq(&self, commit_seq: u64) -> Result<(), CommandLogError>;
    /// Drop every record with `seq <= through_seq` and clamp the header commit index.
    fn truncate_through(&self, through_seq: u64) -> Result<(), CommandLogError>;
    /// Drop every record with `seq >= from_seq`. Used to roll back a failed append; the
    /// header commit index is left alone (a failed append never advanced it).
    fn truncate_from(&self, from_seq: u64) -> Result<(), CommandLogError>;
}

pub struct NoopCommandLogStore;

impl CommandLogStore for NoopCommandLogStore {
    fn load(&self) -> Result<CommandLogState, CommandLogError> {
        Ok(CommandLogState::empty())
    }

    fn append(&self, _seq: u64, _command: &[u8]) -> Result<(), CommandLogError> {
        Ok(())
    }

    fn set_commit_seq(&self, _commit_seq: u64) -> Result<(), CommandLogError> {
        Ok(())
    }

    fn truncate_through(&self, _through_seq: u64) -> Result<(), CommandLogError> {
        Ok(())
    }

    fn truncate_from(&self, _from_seq: u64) -> Result<(), CommandLogError> {
        Ok(())
    }
}

pub struct FileCommandLogStore {
    dir: PathBuf,
}

impl FileCommandLogStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, CommandLogError> {
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

    fn ensure_header(&self) -> Result<(), CommandLogError> {
        let path = self.target();
        if path.exists() {
            return Ok(());
        }
        let mut file = File::create(&path)?;
        write_header(&mut file, 0)?;
        file.sync_all()?;
        Ok(())
    }

    fn rewrite(&self, commit_seq: u64, entries: &BTreeMap<u64, Vec<u8>>) -> Result<(), CommandLogError> {
        let tmp = self.tmp();
        {
            let mut file = File::create(&tmp)?;
            write_header(&mut file, commit_seq)?;
            for (seq, command) in entries {
                write_record(&mut file, *seq, command)?;
            }
            file.sync_all()?;
        }
        fs::rename(&tmp, self.target())?;
        Ok(())
    }
}

impl CommandLogStore for FileCommandLogStore {
    fn load(&self) -> Result<CommandLogState, CommandLogError> {
        let path = self.target();
        if !path.exists() {
            return Ok(CommandLogState::empty());
        }
        let mut file = File::open(&path)?;
        read_log(&mut file)
    }

    fn append(&self, seq: u64, command: &[u8]) -> Result<(), CommandLogError> {
        self.ensure_header()?;
        let mut file = OpenOptions::new().append(true).open(self.target())?;
        write_record(&mut file, seq, command)?;
        file.sync_all()?;
        Ok(())
    }

    fn set_commit_seq(&self, commit_seq: u64) -> Result<(), CommandLogError> {
        self.ensure_header()?;
        let mut file = OpenOptions::new().write(true).open(self.target())?;
        file.seek(SeekFrom::Start(5))?;
        write_u64_be(&mut file, commit_seq)?;
        file.sync_all()?;
        Ok(())
    }

    fn truncate_through(&self, through_seq: u64) -> Result<(), CommandLogError> {
        let path = self.target();
        if !path.exists() {
            return Ok(());
        }
        let state = {
            let mut file = File::open(&path)?;
            read_log(&mut file)?
        };
        let kept: BTreeMap<u64, Vec<u8>> = state
            .entries
            .into_iter()
            .filter(|(seq, _)| *seq > through_seq)
            .collect();
        let new_commit = state.commit_seq.min(through_seq);
        self.rewrite(new_commit, &kept)
    }

    fn truncate_from(&self, from_seq: u64) -> Result<(), CommandLogError> {
        let path = self.target();
        if !path.exists() {
            return Ok(());
        }
        let state = {
            let mut file = File::open(&path)?;
            read_log(&mut file)?
        };
        let kept: BTreeMap<u64, Vec<u8>> = state
            .entries
            .into_iter()
            .filter(|(seq, _)| *seq < from_seq)
            .collect();
        self.rewrite(state.commit_seq, &kept)
    }
}

fn read_log<R: Read>(mut r: R) -> Result<CommandLogState, CommandLogError> {
    let magic = read_u32_be(&mut r)?;
    if magic != MAGIC {
        return Err(CommandLogError::BadMagic { got: magic });
    }
    let version = read_u8(&mut r)?;
    if version != VERSION {
        return Err(CommandLogError::BadVersion { version });
    }
    let commit_seq = read_u64_be(&mut r)?;
    let mut entries = BTreeMap::new();
    loop {
        match read_u64_be(&mut r) {
            Ok(seq) => {
                let len = read_i32_be(&mut r)?;
                if len < 0 {
                    return Err(CommandLogError::NegativeLength { seq });
                }
                let len = len as usize;
                let mut command = vec![0u8; len];
                r.read_exact(&mut command).map_err(map_eof)?;
                entries.insert(seq, command);
            }
            Err(CommandLogError::Truncated) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(CommandLogState { commit_seq, entries })
}

fn write_header<W: Write>(w: &mut W, commit_seq: u64) -> Result<(), CommandLogError> {
    write_u32_be(w, MAGIC)?;
    write_u8(w, VERSION)?;
    write_u64_be(w, commit_seq)?;
    Ok(())
}

fn write_record<W: Write>(w: &mut W, seq: u64, command: &[u8]) -> Result<(), CommandLogError> {
    write_u64_be(w, seq)?;
    write_i32_be(w, command.len() as i32)?;
    w.write_all(command).map_err(CommandLogError::Io)?;
    Ok(())
}

fn read_u8(r: &mut impl Read) -> Result<u8, CommandLogError> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(b[0])
}

fn read_u32_be(r: &mut impl Read) -> Result<u32, CommandLogError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(u32::from_be_bytes(b))
}

fn read_i32_be(r: &mut impl Read) -> Result<i32, CommandLogError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(i32::from_be_bytes(b))
}

fn read_u64_be(r: &mut impl Read) -> Result<u64, CommandLogError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b).map_err(map_eof)?;
    Ok(u64::from_be_bytes(b))
}

fn write_u8(w: &mut impl Write, v: u8) -> Result<(), CommandLogError> {
    w.write_all(&[v]).map_err(CommandLogError::Io)
}

fn write_u32_be(w: &mut impl Write, v: u32) -> Result<(), CommandLogError> {
    w.write_all(&v.to_be_bytes()).map_err(CommandLogError::Io)
}

fn write_i32_be(w: &mut impl Write, v: i32) -> Result<(), CommandLogError> {
    w.write_all(&v.to_be_bytes()).map_err(CommandLogError::Io)
}

fn write_u64_be(w: &mut impl Write, v: u64) -> Result<(), CommandLogError> {
    w.write_all(&v.to_be_bytes()).map_err(CommandLogError::Io)
}

fn map_eof(e: io::Error) -> CommandLogError {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        CommandLogError::Truncated
    } else {
        CommandLogError::Io(e)
    }
}

/// Encode header + one record for golden-vector tests.
pub fn encode_for_test(entry: &CommandLogEntry, commit_seq: u64) -> Result<Vec<u8>, CommandLogError> {
    let mut buf = Vec::new();
    write_header(&mut buf, commit_seq)?;
    write_record(&mut buf, entry.seq, &entry.command)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dref-command-log-{label}-{}-{}",
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
        let store = FileCommandLogStore::open(&dir).unwrap();
        assert_eq!(store.load().unwrap(), CommandLogState::empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_and_load_round_trip() {
        let dir = temp_dir("round-trip");
        let _ = fs::remove_dir_all(&dir);
        let store = FileCommandLogStore::open(&dir).unwrap();
        store.append(1, &[1, 2, 3]).unwrap();
        store.append(2, &[4]).unwrap();
        store.set_commit_seq(1).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.commit_seq, 1);
        assert_eq!(loaded.entries.get(&1).map(|v| v.as_slice()), Some(&[1, 2, 3][..]));
        assert_eq!(loaded.entries.get(&2).map(|v| v.as_slice()), Some(&[4][..]));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_from_drops_failed_suffix_and_keeps_commit() {
        let dir = temp_dir("truncate-from");
        let _ = fs::remove_dir_all(&dir);
        let store = FileCommandLogStore::open(&dir).unwrap();
        store.append(1, &[1]).unwrap();
        store.append(2, &[2]).unwrap();
        store.append(3, &[3]).unwrap();
        store.set_commit_seq(2).unwrap();
        // Roll back the failed append at seq=3.
        store.truncate_from(3).unwrap();
        let loaded = store.load().unwrap();
        // commit index is untouched by a rollback.
        assert_eq!(loaded.commit_seq, 2);
        assert!(loaded.entries.contains_key(&1));
        assert!(loaded.entries.contains_key(&2));
        assert!(!loaded.entries.contains_key(&3));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_through_drops_committed_prefix() {
        let dir = temp_dir("truncate");
        let _ = fs::remove_dir_all(&dir);
        let store = FileCommandLogStore::open(&dir).unwrap();
        store.append(1, &[1]).unwrap();
        store.append(2, &[2]).unwrap();
        store.set_commit_seq(2).unwrap();
        store.truncate_through(1).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.commit_seq, 1);
        assert!(!loaded.entries.contains_key(&1));
        assert_eq!(loaded.entries.get(&2).map(|v| v.as_slice()), Some(&[2][..]));
        let _ = fs::remove_dir_all(&dir);
    }
}
