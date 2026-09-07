use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use super::format::{Entry, HEADER_SIZE, Header, write_header, write_record};
use super::index::Index;

pub fn create(path: &Path) -> io::Result<()> {
    let mut file = File::create(path)?;

    write_header(
        &mut file,
        &Header {
            index_offset: 0,
            index_len: 0,
            entry_count: 0,
        },
    )?;

    Ok(())
}
/// Normal SSTable writer used when flushing the MemTable.
pub fn write(path: &Path, entries: &[(&String, &Entry)]) -> io::Result<Index> {
    let mut file = File::create(path)?;

    let mut index = Index::new();
    let mut offset = HEADER_SIZE;

    // Placeholder header. It is rewritten after all records and
    // the index have been written.
    write_header(
        &mut file,
        &Header {
            index_offset: 0,
            index_len: 0,
            entry_count: 0,
        },
    )?;

    for (key, entry) in entries {
        index.add((*key).clone(), offset);

        write_record(&mut file, key, entry)?;

        let value_len = match entry {
            Entry::Set(value) => value.len(),
            Entry::Delete => 0,
        };

        offset += 9 + key.len() as u64 + value_len as u64;
    }

    let index_offset = offset;

    for entry in index.iter() {
        let key_bytes = entry.key.as_bytes();

        let key_len = u32::try_from(key_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key is too large"))?;

        file.write_all(&key_len.to_le_bytes())?;
        file.write_all(key_bytes)?;
        file.write_all(&entry.offset.to_le_bytes())?;

        offset += 4 + key_bytes.len() as u64 + 8;
    }

    let index_len = offset - index_offset;
    let entry_count = index.len() as u64;

    file.seek(SeekFrom::Start(0))?;

    write_header(
        &mut file,
        &Header {
            index_offset,
            index_len,
            entry_count,
        },
    )?;

    file.sync_all()?;

    Ok(index)
}

/// Streaming SSTable writer used by compaction.
///
/// Records are written directly to disk in sorted order.
/// The index is still kept in memory because the current SSTable
/// format stores the index at the end of the file.
pub struct StreamingWriter {
    file: File,
    index: Index,
    offset: u64,
}

impl StreamingWriter {
    pub fn create(path: &Path) -> io::Result<Self> {
        let mut file = File::create_new(path)?;

        write_header(
            &mut file,
            &Header {
                index_offset: 0,
                index_len: 0,
                entry_count: 0,
            },
        )?;

        Ok(Self {
            file,
            index: Index::new(),
            offset: HEADER_SIZE,
        })
    }

    pub fn write_entry(&mut self, key: &str, entry: &Entry) -> io::Result<()> {
        // SSTable records must be strictly sorted.
        if let Some(last) = self.index.iter().last() {
            if last.key.as_str() >= key {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SSTable entries must be sorted",
                ));
            }
        }

        self.index.add(key.to_owned(), self.offset);

        write_record(&mut self.file, key, entry)?;

        let value_len = match entry {
            Entry::Set(value) => value.len(),
            Entry::Delete => 0,
        };

        self.offset += 9 + key.len() as u64 + value_len as u64;

        Ok(())
    }

    pub fn finish(mut self) -> io::Result<Index> {
        let index_offset = self.offset;

        // Write the index at the end of the file.
        for entry in self.index.iter() {
            let key_bytes = entry.key.as_bytes();

            let key_len = u32::try_from(key_bytes.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key is too large"))?;

            self.file.write_all(&key_len.to_le_bytes())?;
            self.file.write_all(key_bytes)?;
            self.file.write_all(&entry.offset.to_le_bytes())?;

            self.offset += 4 + key_bytes.len() as u64 + 8;
        }

        let index_len = self.offset - index_offset;
        let entry_count = self.index.len() as u64;

        // Rewrite the header with the final index metadata.
        self.file.seek(SeekFrom::Start(0))?;

        write_header(
            &mut self.file,
            &Header {
                index_offset,
                index_len,
                entry_count,
            },
        )?;

        self.file.sync_all()?;

        Ok(self.index)
    }
}
