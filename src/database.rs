use std::collections::HashMap;
use std::fs;
use std::io;

use super::log_record::{Command, DataType};
use super::logger::Logger;
use super::sstable::{Entry, SSTable};

pub struct Database {
    data: HashMap<String, Entry>,
    data_threshold: usize,
    log: Logger,
    sstables: Vec<SSTable>,
    sstable_threshold: usize,
    next_sstable_id: u32,
}

impl Database {
    pub fn new(data_threshold: usize, sstable_threshold: usize) -> io::Result<Self> {
        let mut database = Self {
            data: HashMap::new(),
            data_threshold,
            log: Logger::new("database.log".to_string())?,
            sstables: Vec::new(),
            sstable_threshold,
            next_sstable_id: 0,
        };

        database.load_sstables()?;
        database.recover()?;

        Ok(database)
    }

    pub fn insert(&mut self, key: String, value: String) -> io::Result<()> {
        self.log.log(Command::Set, DataType::String, &key, &value)?;

        self.data.insert(key, Entry::Set(value));

        self.maybe_flush()?;
        self.maybe_compact()?;

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        self.log.log(Command::Delete, DataType::String, key, "")?;

        self.data.insert(key.to_owned(), Entry::Delete);

        self.maybe_flush()?;
        self.maybe_compact()?;

        Ok(())
    }

    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        if let Some(entry) = self.data.get(key) {
            return Ok(match entry {
                Entry::Set(value) => Some(value.clone()),
                Entry::Delete => None,
            });
        }

        for sstable in self.sstables.iter().rev() {
            match sstable.read_entry(key)? {
                Some(Entry::Set(value)) => return Ok(Some(value)),
                Some(Entry::Delete) => return Ok(None),
                None => continue,
            }
        }

        Ok(None)
    }

    pub fn recover(&mut self) -> io::Result<()> {
        for record in self.log.read_records()? {
            match record.command() {
                Command::Set => {
                    if let Some(value) = record.value() {
                        self.data
                            .insert(record.key().to_owned(), Entry::Set(value.to_owned()));
                    }
                }

                Command::Delete => {
                    self.data.insert(record.key().to_owned(), Entry::Delete);
                }
            }
        }

        Ok(())
    }

    fn maybe_flush(&mut self) -> io::Result<()> {
        if self.data.len() >= self.data_threshold {
            self.flush_memory_to_disk()?;
        }

        Ok(())
    }

    fn maybe_compact(&mut self) -> io::Result<()> {
        if self.sstables.len() < self.sstable_threshold {
            return Ok(());
        }

        self.compact_all()?;

        Ok(())
    }

    fn compact_all(&mut self) -> io::Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }

        let mut merged = HashMap::new();
        let mut old_paths = Vec::new();

        // Older tables first, newer tables later.
        // Therefore newer values overwrite older values.
        for sstable in &self.sstables {
            old_paths.push(sstable.path().to_path_buf());

            for (key, entry) in sstable.load_entries()? {
                merged.insert(key, entry);
            }
        }

        let compacted_path = format!("sstable_{}.sst", self.next_sstable_id).into();

        let mut entries: Vec<_> = merged.iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));

        let mut compacted = SSTable::new(compacted_path)?;
        compacted.write_entries(&entries)?;

        self.next_sstable_id += 1;

        // Only modify the SSTable list after successful compaction.
        self.sstables.clear();
        self.sstables.push(compacted);

        // Remove old files from disk.
        for path in old_paths {
            fs::remove_file(path)?;
        }

        Ok(())
    }

    fn load_sstables(&mut self) -> io::Result<()> {
        let mut ids = Vec::new();

        for entry in fs::read_dir(".")? {
            let file_name = entry?.file_name();

            let Some(name) = file_name.to_str() else {
                continue;
            };

            let Some(id) = name
                .strip_prefix("sstable_")
                .and_then(|name| name.strip_suffix(".sst"))
                .and_then(|id| id.parse::<u32>().ok())
            else {
                continue;
            };

            ids.push(id);
        }

        ids.sort_unstable();

        for id in ids {
            let file_name = format!("sstable_{id}.sst");

            self.sstables.push(SSTable::open(file_name.into())?);
            self.next_sstable_id = self.next_sstable_id.max(id + 1);
        }

        Ok(())
    }

    fn flush_memory_to_disk(&mut self) -> io::Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }

        let file_name = format!("sstable_{}.sst", self.next_sstable_id);

        let mut sstable = SSTable::new(file_name.into())?;

        let mut entries: Vec<_> = self.data.iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));

        sstable.write_entries(&entries)?;

        self.sstables.push(sstable);
        self.next_sstable_id += 1;

        self.log.truncate()?;
        self.data.clear();

        Ok(())
    }
}
