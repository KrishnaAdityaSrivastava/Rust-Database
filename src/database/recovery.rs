use std::fs;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::lsm::sstable::{Entry, SSTable};
use crate::wal::log_record::Command;
use super::Database;

impl Database {
    pub(crate) fn load_sstables(&mut self) -> io::Result<()> {
        let mut ids = Vec::new();

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let file_name = entry.file_name();

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

        let levels = self.leveled_sstable.read().unwrap();
        if levels.is_empty() {
            return Ok(());
        }
        drop(levels);

        for id in ids {
            let file_name = self.dir.join(format!("sstable_{id}.sst"));
            let sstable = SSTable::open(file_name)?;

            {
                let mut levels = self.leveled_sstable.write().unwrap();
                levels[0].push(Arc::new(sstable));
            }

            /*
             * Ensure future SSTables don't reuse an existing ID.
             */
            self.next_sstable_id.fetch_max(id + 1, Ordering::Relaxed);
        }

        Ok(())
    }

    pub(crate) fn recover(&mut self) -> io::Result<()> {
    let records = {
        let mut log = self.log.lock().unwrap();
        log.read_records()?
    };

    let mut data = self.data.write().unwrap();

    for record in records {
        match record.command() {
            Command::Set { key, value } => {
                data.insert(key.clone(), Entry::Set(value.clone()));
            }

            Command::Delete { key } => {
                data.insert(key.clone(), Entry::Delete);
            }
        }
    }

    Ok(())
}
}
