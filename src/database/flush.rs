use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::lsm::sstable::SSTable;
use super::Database;

impl Database {
    pub(crate) fn maybe_flush(&self) -> io::Result<()> {
        let should_flush = {
            let data = self.data.read().unwrap();
            data.len() >= self.data_threshold
        };

        if should_flush {
            self.flush_memory_to_disk()?;
        }

        Ok(())
    }

    pub(crate) fn flush_memory_to_disk(&self) -> io::Result<()> {
        // Prevent multiple threads from flushing simultaneously.
        let _flush_guard = self.flush_lock.lock().unwrap();

        let mut data = self.data.write().unwrap();

        if data.is_empty() {
            return Ok(());
        }

        // Allocate unique SSTable ID.
        let id = self.next_sstable_id.fetch_add(1, Ordering::Relaxed);
        let file_name = self.dir.join(format!("sstable_{id}.sst"));

        let mut sstable = SSTable::new(file_name)?;

        // Sort MemTable entries before writing.
        let mut entries: Vec<_> = data.iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));

        // Write the complete SSTable.
        sstable.write_entries(&entries)?;

        {
            let mut levels = self.leveled_sstable.write().unwrap();
            if levels.is_empty() {
                return Ok(());
            }
            levels[0].push(Arc::new(sstable));
        }

        /*
         * The SSTable is now published.
         *
         * The WAL can be truncated because the data represented
         * by this MemTable now exists in the SSTable.
         */
        {
            let mut log = self.log.lock().unwrap();
            log.truncate()?;
        }

        /*
         * The MemTable is still write-locked, so no other writer
         * could have inserted data into it while we were flushing.
         */
        data.clear();

        Ok(())
    }
}
