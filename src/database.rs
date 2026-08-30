use std::collections::HashMap;
use std::io;

use super::logger::Logger;
use super::log_record::{Command, DataType};

pub struct Database {
    data: HashMap<String, String>,
    log: Logger,
}

impl Database {
    pub fn new() -> Self {
        let mut database = Database {
            data: HashMap::new(),
            log: Logger::new("database.log".to_string())
                .expect("Failed to create logger"),
        };

        database
            .recover()
            .expect("Failed to recover database");

        database
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.log
            .log(
                Command::Set,
                DataType::String,
                &key,
                &value,
            )
            .expect("Failed to write to log");

        self.data.insert(key, value);
    }

    pub fn delete(&mut self, key: &str) {
        if self.data.contains_key(key) {
            self.log
                .log(
                    Command::Delete,
                    DataType::String,
                    key,
                    "",
                )
                .expect("Failed to write to log");

            self.data.remove(key);
        }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn recover(&mut self) -> io::Result<()> {
        let records = self.log.read_records()?;

        for record in records {
            match record.command() {
                Command::Set => {
                    if let Some(value) = record.value() {
                        self.data.insert(
                            record.key().to_owned(),
                            value.to_owned(),
                        );
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
        for (key, value) in &self.data {
            
        }

        self.data.clear();

        Ok(())
    }
}
