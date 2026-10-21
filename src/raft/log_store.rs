/// Persistent storage for the Raft log.
///
/// Each `LogEntry` is written as a framed record:
///
///   [ 4-byte little-endian payload length | payload (bincode-encoded LogEntry) ]
///
/// Records are appended on every log push and replayed in full on startup.
/// Truncation (for log conflicts or compaction) rewrites the tail of the file.
///
/// This satisfies the Raft requirement that `log`, `current_term`, and
/// `voted_for` must survive crashes (§3.4 of the Raft paper). Term / vote
/// persistence can be layered on top later; the log is the most critical piece
/// for membership-change correctness.
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use super::node::LogEntry;

/// A persistent, append-only store for `LogEntry` values.
pub struct RaftLogStore {
    file: File,
    /// Byte offsets of each entry's start (used for truncation).
    offsets: Vec<u64>,
}

impl RaftLogStore {
    /// Open (or create) the log store at `path`.
    ///
    /// Returns the store along with all previously-persisted entries so the
    /// caller can restore `RaftNode::log` from them.
    pub fn open(path: impl AsRef<Path>) -> io::Result<(Self, Vec<LogEntry>)> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let mut reader = BufReader::with_capacity(256 * 1024, &file);
        let mut entries = Vec::new();
        let mut offsets = Vec::new();
        let mut truncate_at: Option<u64> = None;

        loop {
            let pos = reader.stream_position()?;

            match read_entry(&mut reader) {
                Ok(Some(entry)) => {
                    offsets.push(pos);
                    entries.push(entry);
                }
                Ok(None) => break,
                Err(e) => {
                    // Corrupt tail — truncate and stop. This can happen after an
                    // unclean shutdown while a write was in-flight.
                    eprintln!(
                        "[RaftLogStore] Corruption at byte offset {pos}: {e}. \
                         Truncating tail."
                    );
                    truncate_at = Some(pos);
                    break;
                }
            }
        }

        drop(reader);

        if let Some(trunc) = truncate_at {
            file.set_len(trunc)?;
        }

        // Position at end for future appends.
        file.seek(SeekFrom::End(0))?;

        Ok((Self { file, offsets }, entries))
    }

    /// Append one entry to the store.
    ///
    /// The write is flushed and synced before returning so that a crash after
    /// this call cannot lose the entry.
    pub fn append(&mut self, entry: &LogEntry) -> io::Result<()> {
        let payload = bincode::serialize(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let len = payload.len() as u32;

        let pos = self.file.stream_position()?;

        {
            let mut writer = BufWriter::new(&mut self.file);
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(&payload)?;
            writer.flush()?;
        } // BufWriter dropped here, releasing the mutable borrow

        // fsync so the kernel buffer is flushed to disk.
        self.file.sync_data()?;

        self.offsets.push(pos);
        Ok(())
    }

    /// Remove all entries at index `from` (0-based, inclusive) and beyond.
    ///
    /// This is called when the leader sends entries that conflict with our log,
    /// requiring us to roll back.  After truncation the file is positioned at
    /// the new end so subsequent `append` calls are correct.
    pub fn truncate_from(&mut self, from: usize) -> io::Result<()> {
        if from >= self.offsets.len() {
            return Ok(()); // Nothing to truncate.
        }

        let new_end = self.offsets[from];
        self.file.set_len(new_end)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.sync_data()?;

        self.offsets.truncate(from);
        Ok(())
    }

    /// How many entries are currently persisted.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }
}

// ---------- Framing helpers ----------

fn read_entry<R: Read>(reader: &mut R) -> io::Result<Option<LogEntry>> {
    let mut len_buf = [0u8; 4];

    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let len = u32::from_le_bytes(len_buf) as usize;

    // Sanity-check: reject obviously corrupt length fields (> 64 MiB).
    const MAX_ENTRY_SIZE: usize = 64 * 1024 * 1024;
    if len > MAX_ENTRY_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("entry length {len} exceeds maximum {MAX_ENTRY_SIZE}"),
        ));
    }

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;

    let entry: LogEntry = bincode::deserialize(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Some(entry))
}

// ---------- RaftMeta ----------

/// Durable storage for the three pieces of Raft state that must survive crashes
/// alongside the log:
///
/// - `commit_index` — the highest log index known to be committed by a quorum.
///   Persisting this lets a restarted node replay all committed entries (including
///   `AddNode`/`RemoveNode`) without waiting for the leader.
/// - `current_term` — prevents a node from voting twice in the same term.
/// - `voted_for`    — same invariant; stored as `u64` with `0` meaning `None`.
///
/// The file layout is a single fixed-size record:
///
///   [ 8-byte LE commit_index | 8-byte LE current_term | 8-byte LE voted_for ]
///
/// Writing is atomic at the OS page level for aligned 8-byte values on Linux;
/// for extra safety we `sync_data()` after every write.
pub struct RaftMeta {
    file: File,
}

/// Values loaded from a `RaftMeta` file on startup.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetaState {
    pub commit_index: usize,
    pub current_term: u64,
    /// `None` means the node has not voted in `current_term`.
    pub voted_for: Option<u64>,
}

const META_SIZE: usize = 24; // 3 × 8 bytes

impl RaftMeta {
    /// Open (or create) the metadata file at `path`.
    ///
    /// Returns the store and the last-written state.  On a fresh or corrupt
    /// file the returned `MetaState` has all fields set to their zero values.
    pub fn open(path: impl AsRef<Path>) -> io::Result<(Self, MetaState)> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let mut buf = [0u8; META_SIZE];
        let state = match file.read_exact(&mut buf) {
            Ok(()) => {
                let commit_index = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
                let current_term = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                let voted_for_raw = u64::from_le_bytes(buf[16..24].try_into().unwrap());
                let voted_for = if voted_for_raw == 0 {
                    None
                } else {
                    Some(voted_for_raw)
                };
                MetaState {
                    commit_index,
                    current_term,
                    voted_for,
                }
            }
            // Fresh or incomplete file — start from defaults.
            Err(_) => MetaState::default(),
        };

        Ok((Self { file }, state))
    }

    /// Atomically overwrite the metadata record.
    ///
    /// `voted_for` uses `0` as the sentinel for "no vote"; node IDs must
    /// therefore start at `1` (which they do — see `NodeId`).
    pub fn save(
        &mut self,
        commit_index: usize,
        current_term: u64,
        voted_for: Option<u64>,
    ) -> io::Result<()> {
        let mut buf = [0u8; META_SIZE];
        buf[0..8].copy_from_slice(&(commit_index as u64).to_le_bytes());
        buf[8..16].copy_from_slice(&current_term.to_le_bytes());
        buf[16..24].copy_from_slice(&voted_for.unwrap_or(0).to_le_bytes());

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&buf)?;
        self.file.sync_data()?;

        Ok(())
    }
}
