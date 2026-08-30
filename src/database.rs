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
    next_sstable_id: u32,
}

impl Database {
    pub fn new(data_threshold: usize) -> Self {
        let mut database = Self {
            data: HashMap::new(),
            data_threshold,
            log: Logger::new("database.log".to_string()).expect("Failed to create logger"),
            sstables: Vec::new(),
            next_sstable_id: 0,
        };

        database.load_sstables().expect("Failed to load SSTables");

        database.recover().expect("Failed to recover database");

        database
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.log
            .log(Command::Set, DataType::String, &key, &value)
            .expect("Failed to write to log");

        self.data.insert(key, Entry::Set(value));

        if self.data.len() >= self.data_threshold {
            self.flush_memory_to_disk()
                .expect("Failed to flush memory to disk");
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.log
            .log(Command::Delete, DataType::String, key, "")
            .expect("Failed to write to log");

        self.data.insert(key.to_owned(), Entry::Delete);

        if self.data.len() >= self.data_threshold {
            self.flush_memory_to_disk()
                .expect("Failed to flush memory to disk");
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(entry) = self.data.get(key) {
            return match entry {
                Entry::Set(value) => Some(value.clone()),
                Entry::Delete => None,
            };
        }

        for sstable in self.sstables.iter().rev() {
            match sstable.read_entry(key) {
                Ok(Some(Entry::Set(value))) => return Some(value),
                Ok(Some(Entry::Delete)) => return None,
                Ok(None) => continue,
                Err(_) => return None,
            }
        }

        None
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

        // SSTable is synced before removing the WAL.
        self.log.truncate()?;

        self.sstables.push(sstable);
        self.next_sstable_id += 1;
        self.data.clear();

        Ok(())
    }
}
