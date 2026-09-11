use std::fs::File;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use super::format::{write_header, write_record, Entry, Header, HEADER_SIZE};
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
    let file = File::create(path)?;
    let mut file = BufWriter::with_capacity(256 * 1024, file);

    let mut index = Index::new();
    let mut offset = HEADER_SIZE;

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

    // Flush buffered data before seeking back to rewrite the header.
    file.flush()?;

    file.seek(SeekFrom::Start(0))?;

    write_header(
        &mut file,
        &Header {
            index_offset,
            index_len,
            entry_count,
        },
    )?;

    // Ensure the final header and all buffered data reach the OS.
    file.flush()?;
    file.get_ref().sync_all()?;

    Ok(index)
}

/// Streaming SSTable writer used by compaction.
pub struct StreamingWriter {
    file: BufWriter<File>,
    index: Index,
    offset: u64,
}

impl StreamingWriter {
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut file = BufWriter::with_capacity(256 * 1024, file);

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

        // Important: flush before seeking.
        self.file.flush()?;

        self.file.seek(SeekFrom::Start(0))?;

        // Write through BufWriter, not get_mut().
        write_header(
            &mut self.file,
            &Header {
                index_offset,
                index_len,
                entry_count,
            },
        )?;

        // Make sure header + index + records are all flushed.
        self.file.flush()?;

        // sync_all operates on the underlying File.
        self.file.get_ref().sync_all()?;

        Ok(self.index)
    }
}