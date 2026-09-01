use std::collections::HashMap;
use std::fs;
use std::io;

use std::sync::{Arc, RwLock};

use super::lsm::sstable::{Entry, SSTable};
use super::wal::{
    Logger,
    log_record::{Command, DataType},
};

pub struct Database {
    data: HashMap<String, Entry>,
    data_threshold: usize,
    log: Logger,
    leveled_sstable: Vec<Vec<SSTable>>,

    sstable_threshold: usize,
    next_sstable_id: u32,
}


impl Database {
    pub fn new(
        sstable_level: usize,
        data_threshold: usize,
        sstable_threshold: usize,
    ) -> io::Result<Self> {
        let mut database = Self {
            data: HashMap::new(),
            data_threshold,
            log: Logger::new("database.log".to_string())?,
            leveled_sstable: (0..sstable_level).map(|_| Vec::new()).collect(),
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
        // MemTable is always the newest data.
        if let Some(entry) = self.data.get(key) {
            return Ok(match entry {
                Entry::Set(value) => Some(value.clone()),
                Entry::Delete => None,
            });
        }

        for level in &self.leveled_sstable {
            for sstable in level.iter().rev() {
                match sstable.read_entry(key)? {
                    Some(Entry::Set(value)) => return Ok(Some(value)),
                    Some(Entry::Delete) => return Ok(None),
                    None => continue,
                }
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
        if self.leveled_sstable.len() < 2 {
            return Ok(());
        }

        for level in 0..self.leveled_sstable.len() - 1 {
            if self.leveled_sstable[level].len() >= self.sstable_threshold {
                self.compact_level(level)?;
            }
        }

        Ok(())
    }

    fn compact_level(&mut self, level: usize) -> io::Result<()> {
        if level + 1 >= self.leveled_sstable.len() {
            return Ok(());
        }

        if self.leveled_sstable[level].is_empty() {
            return Ok(());
        }

        let mut merged: HashMap<String, Entry> = HashMap::new();
        let mut old_paths = Vec::new();

        for sstable in &self.leveled_sstable[level + 1] {
            old_paths.push(sstable.path().to_path_buf());

            for (key, entry) in sstable.load_entries()? {
                merged.insert(key, entry);
            }
        }

        for sstable in &self.leveled_sstable[level] {
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

        self.leveled_sstable[level + 1].clear();
        self.leveled_sstable[level + 1].push(compacted);

        self.leveled_sstable[level].clear();

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

        if self.leveled_sstable.is_empty() {
            return Ok(());
        }

        // ---------------------------------------------------------
        // For now, load existing SSTables into L0.
        // ---------------------------------------------------------
        //
        // This is enough while developing the level system.
        // Later we should persist the level number in the filename
        // so the database knows exactly which SSTable belongs to
        // which level after restart.
        //
        for id in ids {
            let file_name = format!("sstable_{id}.sst");

            let sstable = SSTable::open(file_name.into())?;

            self.leveled_sstable[0].push(sstable);

            self.next_sstable_id = self.next_sstable_id.max(id + 1);
        }

        Ok(())
    }

    fn flush_memory_to_disk(&mut self) -> io::Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }

        // New SSTables always enter L0.
        let file_name = format!("sstable_{}.sst", self.next_sstable_id);

        let mut sstable = SSTable::new(file_name.into())?;

        let mut entries: Vec<_> = self.data.iter().collect();

        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));

        sstable.write_entries(&entries)?;

        self.leveled_sstable[0].push(sstable);

        self.next_sstable_id += 1;

        // WAL can be truncated only after the SSTable has been
        // successfully written.
        self.log.truncate()?;

        self.data.clear();

        Ok(())
    }
}
