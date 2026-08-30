use std::collections::HashMap;
use std::io;

use super::log_record::{Command, DataType};
use super::logger::Logger;
use super::sstable::SSTable;

pub struct Database {
    data: HashMap<String, String>,
    data_threshold: usize,
    log: Logger,
    sstables: Vec<SSTable>,
}

impl Database {
    pub fn new(data_threshold: usize) -> Self {
        let mut database = Database {
            data: HashMap::new(),
            data_threshold,
            log: Logger::new("database.log".to_string()).expect("Failed to create logger"),
            sstables: Vec::new(),
        };

        database.recover().expect("Failed to recover database");

        database
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.log
            .log(Command::Set, DataType::String, &key, &value)
            .expect("Failed to write to log");

        self.data.insert(key, value);

        if self.data.len() >= self.data_threshold {
            self.flush_memory_to_disk()
                .expect("Failed to flush memory to disk");
        }
    }

    pub fn delete(&mut self, key: &str) {
        if self.data.contains_key(key) {
            self.log
                .log(Command::Delete, DataType::String, key, "")
                .expect("Failed to write to log");

            self.data.remove(key);
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.data.get(key) {
            return Some(value.clone());
        }

        for sstable in self.sstables.iter().rev() {
            if let Ok(Some(value)) = sstable.read_entry(key) {
                return Some(value);
            }
        }

        None
    }

    pub fn recover(&mut self) -> io::Result<()> {
        let records = self.log.read_records()?;

        for record in records {
            match record.command() {
                Command::Set => {
                    if let Some(value) = record.value() {
                        self.data.insert(record.key().to_owned(), value.to_owned());
                    }
                }

                Command::Delete => {
                    self.data.remove(record.key());
                }
            }
        }

        Ok(())
    }

    pub fn flush_memory_to_disk(&mut self) -> io::Result<()> {
        let id = self.sstables.len() as u32;

        let mut sstable = SSTable::new(format!("sstable_{}.sst", id))?;

        sstable.write_entries(&self.data)?;

        self.sstables.push(sstable);

        self.data.clear();

        Ok(())
    }
}
