pub mod compact;
pub mod flush;
pub mod recovery;

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex, RwLock};

use crate::lsm::sstable::{Entry, SSTable};
use crate::wal::{
    Logger,
    log_record::{Command, DataType},
};

pub struct Database {
    pub(crate) dir: PathBuf,
    pub(crate) data: RwLock<HashMap<String, Entry>>,

    pub(crate) log: Mutex<Logger>,

    // Only one thread can perform a flush at a time.
    pub(crate) flush_lock: Mutex<()>,
    
    // Only one thread can perform compaction at a time.
    pub(crate) compaction_lock: Mutex<()>,

    pub(crate) leveled_sstable: RwLock<Vec<Vec<Arc<SSTable>>>>,

    pub(crate) data_threshold: usize,
    pub(crate) sstable_threshold: usize,

    pub(crate) next_sstable_id: AtomicU32,
}

impl Database {
    pub fn new(
        sstable_level: usize,
        data_threshold: usize,
        sstable_threshold: usize,
    ) -> io::Result<Self> {
        Self::open_in_dir(".", sstable_level, data_threshold, sstable_threshold)
    }

    pub fn open_in_dir(
        dir: impl AsRef<Path>,
        sstable_level: usize,
        data_threshold: usize,
        sstable_threshold: usize,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let mut database = Self {
            log: Mutex::new(Logger::new(dir.join("database.log"))?),
            dir,
            data: RwLock::new(HashMap::new()),
            flush_lock: Mutex::new(()),
            compaction_lock: Mutex::new(()),
            leveled_sstable: RwLock::new((0..sstable_level).map(|_| Vec::new()).collect()),
            data_threshold,
            sstable_threshold,
            next_sstable_id: AtomicU32::new(0),
        };

        database.load_sstables()?;
        database.recover()?;

        Ok(database)
    }

    pub fn insert(&self, key: String, value: String) -> io::Result<()> {
        {
            let mut log = self.log.lock().unwrap();
            log.log(Command::Set, DataType::String, &key, &value)?;
        }

        {
            let mut data = self.data.write().unwrap();
            data.insert(key, Entry::Set(value));
        }

        self.maybe_flush()?;
        self.maybe_compact()?;

        Ok(())
    }

    pub fn delete(&self, key: &str) -> io::Result<()> {
        {
            let mut log = self.log.lock().unwrap();
            log.log(Command::Delete, DataType::String, key, "")?;
        }

        {
            let mut data = self.data.write().unwrap();
            data.insert(key.to_owned(), Entry::Delete);
        }

        self.maybe_flush()?;
        self.maybe_compact()?;

        Ok(())
    }

    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        {
            let data = self.data.read().unwrap();
            if let Some(entry) = data.get(key) {
                return Ok(match entry {
                    Entry::Set(value) => Some(value.clone()),
                    Entry::Delete => None,
                });
            }
        }

        let levels = self.leveled_sstable.read().unwrap();

        for level in levels.iter() {
            for sstable in level.iter().rev() {
                match sstable.read_entry(key)? {
                    Some(Entry::Set(value)) => {
                        return Ok(Some(value));
                    }
                    Some(Entry::Delete) => {
                        return Ok(None);
                    }
                    None => continue,
                }
            }
        }

        Ok(None)
    }
}
