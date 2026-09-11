use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Seek, SeekFrom};

use std::path::Path;

use super::log_record::{Command, DataType, LogRecord};

pub struct Logger {
    file: File,
    len: u64,
}

impl Logger {
    pub fn new(file_name: impl AsRef<Path>) -> io::Result<Self> {
        let path = file_name.as_ref();

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        let mut reader = BufReader::with_capacity(256 * 1024, file);

        let mut len = 0;

        loop {
            let record_start = reader.stream_position()?;

            match LogRecord::read_from(&mut reader) {
                Ok(Some(record)) => {
                    len = record.id() + 1;
                }

                Ok(None) => break,

                Err(e) => {
                    eprintln!("WAL corruption detected at byte {}: {}", record_start, e);

                    // Recover the underlying file and truncate the
                    // corrupt record and everything after it.
                    let mut file = reader.into_inner();

                    file.set_len(record_start)?;
                    file.seek(SeekFrom::End(0))?;

                    return Ok(Logger { file, len });
                }
            }
        }

        // Recover the underlying File. No seek-to-end is required for
        // correctness because the file was opened with O_APPEND.
        file = reader.into_inner();

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

    pub fn read_records(&mut self) -> io::Result<Vec<LogRecord>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut reader = BufReader::with_capacity(256 * 1024, &self.file);
        let mut records = Vec::new();

        loop {
            let record_start = reader.stream_position()?;

            match LogRecord::read_from(&mut reader) {
                Ok(Some(record)) => {
                    records.push(record);
                }

                Ok(None) => break,

                Err(e) => {
                    eprintln!("WAL corruption detected at byte {}: {}", record_start, e);

                    self.file.set_len(record_start)?;
                    break;
                }
            }
        }

        Ok(records)
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        self.len = 0;

        Ok(())
    }
}
