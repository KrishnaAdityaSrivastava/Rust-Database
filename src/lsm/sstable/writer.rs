use std::fs::File;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use super::bloom::BloomFilter;
use super::format::{Entry, HEADER_SIZE, Header, write_header, write_record};
use super::index::Index;

const BUFFER_SIZE: usize = 256 * 1024;
const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;
const STREAMING_EXPECTED_ITEMS: usize = 1024;

fn empty_header() -> Header {
    Header {
        index_offset: 0,
        index_len: 0,
        entry_count: 0,
        bloom_offset: 0,
        bloom_len: 0,
    }
}

fn write_index<W: Write>(file: &mut W, index: &Index) -> io::Result<u64> {
    let mut bytes_written = 0u64;

    for entry in index.iter() {
        let key_bytes = entry.key.as_bytes();

        let key_len = u32::try_from(key_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key is too large"))?;

        file.write_all(&key_len.to_le_bytes())?;
        file.write_all(key_bytes)?;
        file.write_all(&entry.offset.to_le_bytes())?;

        bytes_written += 4 + key_bytes.len() as u64 + 8;
    }

    Ok(bytes_written)
}

pub fn create(path: &Path) -> io::Result<()> {
    let mut file = File::create(path)?;
    write_header(&mut file, &empty_header())?;
    file.sync_all()?;
    Ok(())
}

/// Normal SSTable writer used when flushing the MemTable.
pub fn write(path: &Path, entries: &[(&String, &Entry)]) -> io::Result<Index> {
    let file = File::create(path)?;
    let mut file = BufWriter::with_capacity(BUFFER_SIZE, file);

    let mut index = Index::new();
    let mut bloom_filter = BloomFilter::new(entries.len(), BLOOM_FALSE_POSITIVE_RATE);

    let mut offset = HEADER_SIZE;

    write_header(&mut file, &empty_header())?;

    for (key, entry) in entries {
        let record_len = write_record(&mut file, key, entry)?;

        index.add((*key).clone(), offset);
        bloom_filter.insert(key);

        offset = offset
            .checked_add(record_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable offset overflow"))?;
    }

    // Persist Bloom filter immediately after the data records.
    let bloom_offset = offset;
    let bloom_bytes = bloom_filter.to_bytes();

    file.write_all(&bloom_bytes)?;

    offset = offset
        .checked_add(bloom_bytes.len() as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable offset overflow"))?;

    // Persist sorted index after the Bloom filter.
    let index_offset = offset;
    let index_len = write_index(&mut file, &index)?;

    offset = offset
        .checked_add(index_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable offset overflow"))?;

    let header = Header {
        index_offset,
        index_len,
        entry_count: index.len() as u64,
        bloom_offset,
        bloom_len: bloom_bytes.len() as u64,
    };

    // Finalize the header only after writing all sections.
    file.flush()?;
    file.seek(SeekFrom::Start(0))?;
    write_header(&mut file, &header)?;
    file.flush()?;
    file.get_ref().sync_all()?;

    let _ = offset; // The checked final offset documents section accounting.

    Ok(index)
}

/// Streaming SSTable writer used by compaction.
pub struct StreamingWriter {
    file: BufWriter<File>,
    index: Index,
    bloom_filter: BloomFilter,
    offset: u64,
}

impl StreamingWriter {
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut file = BufWriter::with_capacity(BUFFER_SIZE, file);

        write_header(&mut file, &empty_header())?;

        Ok(Self {
            file,
            index: Index::new(),
            bloom_filter: BloomFilter::new(STREAMING_EXPECTED_ITEMS, BLOOM_FALSE_POSITIVE_RATE),
            offset: HEADER_SIZE,
        })
    }

    pub fn write_entry(&mut self, key: &str, entry: &Entry) -> io::Result<()> {
        if let Some(last) = self.index.iter().last()
            && last.key.as_str() >= key
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SSTable entries must be sorted",
            ));
        }

        let record_len = write_record(&mut self.file, key, entry)?;

        self.index.add(key.to_owned(), self.offset);
        self.bloom_filter.insert(key);

        self.offset = self
            .offset
            .checked_add(record_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable offset overflow"))?;

        Ok(())
    }

    pub fn finish(mut self) -> io::Result<Index> {
        // Write Bloom filter before the index, matching the normal writer.
        let bloom_offset = self.offset;
        let bloom_bytes = self.bloom_filter.to_bytes();

        self.file.write_all(&bloom_bytes)?;

        self.offset = self
            .offset
            .checked_add(bloom_bytes.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable offset overflow"))?;

        let index_offset = self.offset;
        let index_len = write_index(&mut self.file, &self.index)?;

        self.offset = self
            .offset
            .checked_add(index_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable offset overflow"))?;

        let header = Header {
            index_offset,
            index_len,
            entry_count: self.index.len() as u64,
            bloom_offset,
            bloom_len: bloom_bytes.len() as u64,
        };

        self.file.flush()?;
        self.file.seek(SeekFrom::Start(0))?;
        write_header(&mut self.file, &header)?;
        self.file.flush()?;
        self.file.get_ref().sync_all()?;

        Ok(self.index)
    }
}
