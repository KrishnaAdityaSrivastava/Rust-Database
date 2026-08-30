use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use super::format::{Entry, HEADER_SIZE, Header, write_header, write_record};
use super::index::Index;

pub fn create(path: &Path) -> io::Result<()> {
    let mut file = File::create_new(path)?;

    write_header(
        &mut file,
        &Header {
            index_offset: 0,
            index_len: 0,
            entry_count: 0,
        },
    )?;

    file.sync_all()?;

    Ok(())
}

pub fn write(path: &Path, entries: &[(&String, &Entry)]) -> io::Result<Index> {
    let mut file = OpenOptions::new().write(true).open(path)?;

    file.seek(SeekFrom::Start(HEADER_SIZE))?;

    let mut index = Index::new();
    let mut previous_key: Option<&str> = None;

    for (key, entry) in entries {
        if let Some(previous_key) = previous_key {
            if previous_key >= key.as_str() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SSTable entries must be sorted by key",
                ));
            }
        }

        previous_key = Some(key);

        let offset = file.stream_position()?;

        index.add((*key).clone(), offset);

        write_record(&mut file, key, entry)?;
    }

    let index_offset = file.stream_position()?;

    for entry in index.iter() {
        let key_bytes = entry.key.as_bytes();

        let key_len = u32::try_from(key_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key is too large"))?;

        // Index entry:
        //
        // [key_len : 4 bytes]
        // [key     : key_len bytes]
        // [offset  : 8 bytes]

        file.write_all(&key_len.to_le_bytes())?;
        file.write_all(key_bytes)?;
        file.write_all(&entry.offset.to_le_bytes())?;
    }

    let index_len = file
        .stream_position()?
        .checked_sub(index_offset)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid index size"))?;

    let entry_count = u64::try_from(index.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many entries"))?;

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
