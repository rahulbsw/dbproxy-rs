use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::task;

use crate::error::ProxyError;

const JOURNAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XaState {
    Started,
    Prepared,
    CommitDecided,
    AbortDecided,
    Completed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaRecord {
    pub version: u8,
    pub xid: String,
    pub participants: Vec<String>,
    pub state: XaState,
    pub unix_millis: u64,
    pub checksum: String,
}

#[derive(Serialize)]
struct ChecksumPayload<'a> {
    version: u8,
    xid: &'a str,
    participants: &'a [String],
    state: XaState,
    unix_millis: u64,
}

struct JournalState {
    file: File,
    outstanding: HashMap<String, XaRecord>,
}

struct JournalInner {
    path: PathBuf,
    max_bytes: u64,
    state: Mutex<JournalState>,
}

#[derive(Clone)]
pub struct XaJournal {
    inner: Arc<JournalInner>,
}

impl XaJournal {
    pub async fn open(path: PathBuf, max_bytes: u64) -> Result<Self, ProxyError> {
        task::spawn_blocking(move || Self::open_blocking(path, max_bytes))
            .await
            .map_err(|error| ProxyError::Protocol(format!("XA journal task failed: {error}")))?
    }

    fn open_blocking(path: PathBuf, max_bytes: u64) -> Result<Self, ProxyError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| journal_io(&path, error))?;
        }
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .map_err(|error| journal_io(&path, error))?;
        file.try_lock_exclusive().map_err(|error| {
            ProxyError::Config(format!(
                "cannot acquire exclusive XA journal lock for {}: {error}",
                path.display()
            ))
        })?;

        let mut outstanding = HashMap::new();
        for (index, line) in BufReader::new(&file).lines().enumerate() {
            let line = line.map_err(|error| journal_io(&path, error))?;
            if line.trim().is_empty() {
                continue;
            }
            let record: XaRecord = serde_json::from_str(&line).map_err(|error| {
                ProxyError::Config(format!(
                    "corrupt XA journal {} at line {}: {error}",
                    path.display(),
                    index + 1
                ))
            })?;
            validate_record(&path, index + 1, &record)?;
            if record.state == XaState::Completed {
                outstanding.remove(&record.xid);
            } else {
                outstanding.insert(record.xid.clone(), record);
            }
        }

        Ok(Self {
            inner: Arc::new(JournalInner {
                path,
                max_bytes,
                state: Mutex::new(JournalState { file, outstanding }),
            }),
        })
    }

    pub async fn append(
        &self,
        xid: &str,
        participants: &[String],
        state: XaState,
    ) -> Result<(), ProxyError> {
        let inner = self.inner.clone();
        let xid = xid.to_owned();
        let participants = participants.to_vec();
        task::spawn_blocking(move || append_blocking(&inner, xid, participants, state))
            .await
            .map_err(|error| ProxyError::Protocol(format!("XA journal task failed: {error}")))?
    }

    pub fn outstanding(&self) -> Result<Vec<XaRecord>, ProxyError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| ProxyError::Protocol("XA journal mutex was poisoned".to_owned()))?;
        let mut records = state.outstanding.values().cloned().collect::<Vec<_>>();
        records.sort_by(|left, right| left.xid.cmp(&right.xid));
        Ok(records)
    }

    pub fn outstanding_count(&self) -> Result<usize, ProxyError> {
        Ok(self.outstanding()?.len())
    }
}

fn append_blocking(
    inner: &JournalInner,
    xid: String,
    participants: Vec<String>,
    state: XaState,
) -> Result<(), ProxyError> {
    let mut guard = inner
        .state
        .lock()
        .map_err(|_| ProxyError::Protocol("XA journal mutex was poisoned".to_owned()))?;
    let mut record = XaRecord {
        version: JOURNAL_VERSION,
        xid,
        participants,
        state,
        unix_millis: unix_millis(),
        checksum: String::new(),
    };
    record.checksum = checksum(&record)?;
    let mut encoded = serde_json::to_vec(&record).map_err(|error| {
        ProxyError::Protocol(format!("cannot encode XA journal record: {error}"))
    })?;
    encoded.push(b'\n');

    let projected = guard
        .file
        .metadata()
        .map_err(|error| journal_io(&inner.path, error))?
        .len()
        .saturating_add(encoded.len() as u64);
    if projected > inner.max_bytes {
        compact(&inner.path, &mut guard)?;
    }
    let compacted_len = guard
        .file
        .metadata()
        .map_err(|error| journal_io(&inner.path, error))?
        .len();
    if compacted_len.saturating_add(encoded.len() as u64) > inner.max_bytes {
        return Err(ProxyError::Protocol(format!(
            "XA journal {} reached its configured {} byte limit",
            inner.path.display(),
            inner.max_bytes
        )));
    }

    guard
        .file
        .seek(SeekFrom::End(0))
        .and_then(|_| guard.file.write_all(&encoded))
        .and_then(|_| guard.file.flush())
        .and_then(|_| guard.file.sync_data())
        .map_err(|error| journal_io(&inner.path, error))?;
    if state == XaState::Completed {
        guard.outstanding.remove(&record.xid);
    } else {
        guard.outstanding.insert(record.xid.clone(), record);
    }
    Ok(())
}

fn compact(path: &Path, state: &mut JournalState) -> Result<(), ProxyError> {
    state
        .file
        .set_len(0)
        .map_err(|error| journal_io(path, error))?;
    state
        .file
        .seek(SeekFrom::Start(0))
        .map_err(|error| journal_io(path, error))?;
    let mut records = state.outstanding.values().cloned().collect::<Vec<_>>();
    records.sort_by(|left, right| left.xid.cmp(&right.xid));
    for record in records {
        serde_json::to_writer(&mut state.file, &record).map_err(|error| {
            ProxyError::Protocol(format!(
                "cannot compact XA journal {}: {error}",
                path.display()
            ))
        })?;
        state
            .file
            .write_all(b"\n")
            .map_err(|error| journal_io(path, error))?;
    }
    state
        .file
        .flush()
        .and_then(|_| state.file.sync_data())
        .map_err(|error| journal_io(path, error))
}

fn validate_record(path: &Path, line: usize, record: &XaRecord) -> Result<(), ProxyError> {
    if record.version != JOURNAL_VERSION || record.xid.is_empty() || record.participants.is_empty()
    {
        return Err(ProxyError::Config(format!(
            "invalid XA journal record in {} at line {line}",
            path.display()
        )));
    }
    let expected = checksum(record)?;
    if record.checksum != expected {
        return Err(ProxyError::Config(format!(
            "XA journal checksum mismatch in {} at line {line}",
            path.display()
        )));
    }
    Ok(())
}

fn checksum(record: &XaRecord) -> Result<String, ProxyError> {
    let payload = ChecksumPayload {
        version: record.version,
        xid: &record.xid,
        participants: &record.participants,
        state: record.state,
        unix_millis: record.unix_millis,
    };
    let bytes = serde_json::to_vec(&payload)
        .map_err(|error| ProxyError::Protocol(format!("cannot checksum XA record: {error}")))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn journal_io(path: &Path, error: std::io::Error) -> ProxyError {
    ProxyError::Config(format!("XA journal {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn journal_replays_only_incomplete_transactions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("xa.jsonl");
        let journal = XaJournal::open(path.clone(), 1_048_576).await.unwrap();
        journal
            .append("x1", &["a".to_owned(), "b".to_owned()], XaState::Started)
            .await
            .unwrap();
        journal
            .append(
                "x1",
                &["a".to_owned(), "b".to_owned()],
                XaState::CommitDecided,
            )
            .await
            .unwrap();
        journal
            .append("x2", &["a".to_owned()], XaState::Started)
            .await
            .unwrap();
        journal
            .append("x2", &["a".to_owned()], XaState::Completed)
            .await
            .unwrap();
        drop(journal);

        let reopened = XaJournal::open(path, 1_048_576).await.unwrap();
        let outstanding = reopened.outstanding().unwrap();
        assert_eq!(outstanding.len(), 1);
        assert_eq!(outstanding[0].xid, "x1");
        assert_eq!(outstanding[0].state, XaState::CommitDecided);
    }

    #[tokio::test]
    async fn journal_rejects_checksum_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("xa.jsonl");
        let journal = XaJournal::open(path.clone(), 1_048_576).await.unwrap();
        journal
            .append("x1", &["a".to_owned()], XaState::Started)
            .await
            .unwrap();
        drop(journal);
        let contents = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, contents.replace("started", "prepared")).unwrap();
        assert!(XaJournal::open(path, 1_048_576).await.is_err());
    }
}
