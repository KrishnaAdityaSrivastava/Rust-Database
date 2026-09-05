use std::collections::HashMap;
use std::fs;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::lsm::sstable::{Entry, SSTable};
use super::Database;

impl Database {
    pub(crate) fn maybe_compact(&self) -> io::Result<()> {
        let _compaction_guard = self.compaction_lock.lock().unwrap();

        let levels = self.leveled_sstable.read().unwrap();

        if levels.len() < 2 {
            return Ok(());
        }

        let should_compact: Vec<bool> = (0..levels.len() - 1)
            .map(|level| levels[level].len() >= self.sstable_threshold)
            .collect();

        drop(levels);

        for (level, should) in should_compact.into_iter().enumerate() {
            if should {
                self.compact_level(level)?;
            }
        }

        Ok(())
    }

    pub(crate) fn compact_level(&self, level: usize) -> io::Result<()> {
        let start = std::time::Instant::now();
        /*
         * Take Arc snapshots of the SSTables.
         *
         * We do not keep the RwLock while doing disk I/O.
         */
        let (current_level, next_level) = {
            let levels = self.leveled_sstable.read().unwrap();
            if level + 1 >= levels.len() {
                return Ok(());
            }
            if levels[level].is_empty() {
                return Ok(());
            }
            (levels[level].clone(), levels[level + 1].clone())
        };

        /*
         * Because these are Arc<SSTable>, the SSTable objects remain
         * alive even if another thread later removes them from
         * leveled_sstable.
         */
        let mut merged: HashMap<String, Entry> = HashMap::new();
        let mut old_paths = Vec::new();

        /*
         * First merge the older level.
         * Level N+1 is older than level N.
         */
        for sstable in &next_level {
            old_paths.push(sstable.path().to_path_buf());
            for (key, entry) in sstable.load_entries()? {
                merged.insert(key, entry);
            }
        }

        /*
         * Then merge the current level.
         * This overwrites older entries with newer entries.
         */
        for sstable in &current_level {
            old_paths.push(sstable.path().to_path_buf());
            for (key, entry) in sstable.load_entries()? {
                merged.insert(key, entry);
            }
        }

        /*
         * Allocate a new SSTable ID atomically.
         */
        let id = self.next_sstable_id.fetch_add(1, Ordering::Relaxed);
        let compacted_path = self.dir.join(format!("sstable_{id}.sst"));

        let mut entries: Vec<_> = merged.iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));

        /*
         * Create and completely write the new SSTable
         * before touching the existing SSTable lists.
         */
        let mut compacted = SSTable::new(compacted_path)?;
        compacted.write_entries(&entries)?;

        /*
         * Atomically publish the new SSTable.
         * No reader can observe a partially updated level.
         */
        {
            let mut levels = self.leveled_sstable.write().unwrap();
            
            if level + 1 >= levels.len() {
                return Ok(());
            }

            /*
             * Replace only the compacted tables.
             * Concurrent flushes may have added new tables to levels[level],
             * so we use drain to remove only what we compacted.
             */
            levels[level].drain(0..current_level.len());
            levels[level + 1].drain(0..next_level.len());
            levels[level + 1].push(Arc::new(compacted));
        }

        /*
         * The new SSTable is now visible to readers.
         * Remove the old files afterward.
         */
        for path in old_paths {
            fs::remove_file(path)?;
        }

        self.total_compactions.fetch_add(1, Ordering::Relaxed);
        let elapsed_micros = start.elapsed().as_micros() as u64;
        self.compaction_duration_micros.fetch_add(elapsed_micros, Ordering::Relaxed);

        Ok(())
    }
}
