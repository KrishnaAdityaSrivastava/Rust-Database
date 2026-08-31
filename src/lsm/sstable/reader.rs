use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::path::Path;

use super::format::{Entry, read_header, read_record};
use super::index::{Index, read_index};

use std::collections::HashMap;


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

    read_record(&mut file)
}

pub fn load_entries(path: &Path, index: &Index) -> io::Result<HashMap<String, Entry>> {
    let mut file = File::open(path)?;

    let mut entries = HashMap::with_capacity(index.len());

    for index_entry in index.iter() {
        file.seek(SeekFrom::Start(index_entry.offset))?;

        let entry = read_record(&mut file)?;

        let entry = match entry {
            Some(entry) => entry,

            None => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected end of SSTable",
                ));
            }
        };

        entries.insert(index_entry.key.clone(), entry);
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

    if header.index_offset < super::format::HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SSTable index overlaps header",
        ));
    }

    if index_end > file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SSTable index extends beyond file",
        ));
    }

    Ok(())
}
