use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};

use std::path::Path;

use super::log_record::{Command, DataType, LogRecord};

pub struct Logger {
    file: File,
    len: u64,
}

impl Logger {
    pub fn new(file_name: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)?;

        let mut len = 0;

        // Start reading from the beginning of the WAL.
        file.seek(SeekFrom::Start(0))?;

        loop {
            let record_start = file.stream_position()?;

            match LogRecord::read_from(&mut file) {
                Ok(Some(record)) => {
                    // The next record ID should be one greater
                    // than the largest valid record ID.
                    len = record.id() + 1;
                }

                Ok(None) => {
                    break;
                }

                Err(e) => {
                    eprintln!("WAL corruption detected at byte {}: {}", record_start, e);

                    // Remove the corrupt record and everything after it.
                    file.set_len(record_start)?;

                    break;
                }
            }
        }

        // Always leave the cursor at the end so the logger
        // is ready for appending new records.
        file.seek(SeekFrom::End(0))?;

        Ok(Logger { file, len })
    }

    pub fn log(
        &mut self,
        command: Command,
        data_type: DataType,
        key: &str,
        val: &str,
    ) -> io::Result<()> {
        let value = match command {
            Command::Set => Some(val.to_owned()),
            Command::Delete => None,
        };

        let record = LogRecord::new(self.len, command, data_type, key.to_owned(), value);

        record.write_to(&mut self.file)?;

        self.len += 1;

        Ok(())
    }

    /// Read all valid records from the WAL.
    ///
    /// If a corrupt/incomplete record is encountered, it is removed
    /// together with everything after it.
    pub fn read_records(&mut self) -> io::Result<Vec<LogRecord>> {
        let mut records = Vec::new();

        // Start from the beginning of the WAL.
        self.file.seek(SeekFrom::Start(0))?;

        loop {
            let record_start = self.file.stream_position()?;

            match LogRecord::read_from(&mut self.file) {
                Ok(Some(record)) => {
                    records.push(record);
                }

                Ok(None) => {
                    break;
                }

                Err(e) => {
                    eprintln!("WAL corruption detected at byte {}: {}", record_start, e);

                    // Truncate the invalid record and everything after it.
                    self.file.set_len(record_start)?;

                    break;
                }
            }
        }

        // Return the logger to append mode.
        self.file.seek(SeekFrom::End(0))?;

        Ok(records)
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;

        self.len = 0;

        Ok(())
    }
}
