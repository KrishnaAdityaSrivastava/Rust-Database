use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::fs;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::lsm::sstable::{Entry, SSTable};

use super::Database;

struct MergeItem {
    key: String,
    entry: Entry,
    source: usize,
    priority: usize,
}

impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.priority == other.priority
    }
}

impl Eq for MergeItem {}

impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match other.key.cmp(&self.key) {
            CmpOrdering::Equal => self.priority.cmp(&other.priority),
            ordering => ordering,
        }
    }
}

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

        let current_len = current_level.len();

        let mut old_paths = Vec::with_capacity(current_level.len() + next_level.len());

        for sstable in &current_level {
            old_paths.push(sstable.path().to_path_buf());
        }

        for sstable in &next_level {
            old_paths.push(sstable.path().to_path_buf());
        }

        // Open one sequential reader for every input SSTable.
        let mut readers = Vec::with_capacity(current_level.len() + next_level.len());

        for sstable in &current_level {
            readers.push(sstable.sequential_reader()?);
        }

        for sstable in &next_level {
            readers.push(sstable.sequential_reader()?);
        }

        let id = self.next_sstable_id.fetch_add(1, Ordering::Relaxed);

        let compacted_path = self.dir.join(format!("sstable_{id}.sst"));

        let mut writer = SSTable::streaming_writer(&compacted_path)?;

        let mut heap = BinaryHeap::new();

        // Insert the first entry from every SSTable.
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some((key, entry)) = reader.next()? {
                let priority = if source < current_len {
                    // Current level has higher priority.
                    source + current_len
                } else {
                    // Next level has lower priority.
                    source - current_len
                };

                heap.push(MergeItem {
                    key,
                    entry,
                    source,
                    priority,
                });
            }
        }

        // K-way merge.
        while let Some(item) = heap.pop() {
            let key = item.key;

            // The highest-priority version of this key wins.
            writer.write_entry(&key, &item.entry)?;

            // Advance the source that produced the winning entry.
            if let Some((next_key, next_entry)) = readers[item.source].next()? {
                let priority = if item.source < current_len {
                    item.source + current_len
                } else {
                    item.source - current_len
                };

                heap.push(MergeItem {
                    key: next_key,
                    entry: next_entry,
                    source: item.source,
                    priority,
                });
            }

            // Consume all lower-priority versions of this key.
            while let Some(next) = heap.peek() {
                if next.key != key {
                    break;
                }

                let duplicate = heap.pop().unwrap();

                if let Some((next_key, next_entry)) = readers[duplicate.source].next()? {
                    let priority = if duplicate.source < current_len {
                        duplicate.source + current_len
                    } else {
                        duplicate.source - current_len
                    };

                    heap.push(MergeItem {
                        key: next_key,
                        entry: next_entry,
                        source: duplicate.source,
                        priority,
                    });
                }
            }
        }

        // Finalize the SSTable and write its index/header.
        let index = writer.finish()?;

        let compacted = SSTable::from_parts(compacted_path.clone(), index)?;

        // Publish the new SSTable.
        {
            let mut levels = self.leveled_sstable.write().unwrap();

            if level + 1 >= levels.len() {
                return Ok(());
            }

            levels[level].drain(0..current_level.len());

            levels[level + 1].drain(0..next_level.len());

            levels[level + 1].push(Arc::new(compacted));
        }

        // Delete old SSTables after the new table is published.
        for path in old_paths {
            fs::remove_file(path)?;
        }

        self.total_compactions.fetch_add(1, Ordering::Relaxed);

        let elapsed_micros = start.elapsed().as_micros() as u64;

        self.compaction_duration_micros
            .fetch_add(elapsed_micros, Ordering::Relaxed);

        Ok(())
    }
}
