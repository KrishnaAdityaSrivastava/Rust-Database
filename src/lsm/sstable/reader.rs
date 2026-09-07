use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::path::Path;

use super::format::{Entry, HEADER_SIZE, read_header, read_record};
use super::index::{Index, read_index};

pub struct SequentialReader {
    file: File,
    end: u64,
    entries_read: u64,
    entry_count: u64,
}

impl SequentialReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;

        let header = read_header(&mut file)?;

        validate_header(&file, &header)?;

        file.seek(SeekFrom::Start(HEADER_SIZE))?;

        Ok(Self {
            file,
            end: header.index_offset,
            entries_read: 0,
            entry_count: header.entry_count,
        })
    }

    pub fn next(&mut self) -> io::Result<Option<(String, Entry)>> {
        if self.entries_read >= self.entry_count {
            return Ok(None);
        }

        let position = self.file.stream_position()?;

        if position >= self.end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "SSTable data ended before entry_count",
            ));
        }

        let record = read_record(&mut self.file)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of SSTable")
        })?;

        self.entries_read += 1;

        Ok(Some(record))
    }
}

pub fn load_index(path: &Path) -> io::Result<Index> {
    let mut file = File::open(path)?;

    let header = read_header(&mut file)?;

    validate_header(&file, &header)?;

    file.seek(SeekFrom::Start(header.index_offset))?;

    read_index(&mut file, header.index_len, header.entry_count)
}

pub fn read_entry(path: &Path, index: &Index, key: &str) -> io::Result<Option<Entry>> {
    let offset = match index.find(key) {
        Some(offset) => offset,
        None => return Ok(None),
    };

    let mut file = File::open(path)?;

    file.seek(SeekFrom::Start(offset))?;

    match read_record(&mut file)? {
        Some((_, entry)) => Ok(Some(entry)),
        None => Ok(None),
    }
}

pub fn load_entries_sequential(path: &Path) -> io::Result<Vec<(String, Entry)>> {
    let mut reader = SequentialReader::open(path)?;

    let mut entries = Vec::new();

    while let Some(entry) = reader.next()? {
        entries.push(entry);
    }

    Ok(entries)
}

fn validate_header(file: &File, header: &super::format::Header) -> io::Result<()> {
    let file_len = file.metadata()?.len();

    let index_end = header
        .index_offset
        .checked_add(header.index_len)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "SSTable index range overflows")
        })?;

    if header.index_offset < HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid SSTable index offset",
        ));
    }

    if index_end > file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SSTable index exceeds file size",
        ));
    }

    Ok(())
}
