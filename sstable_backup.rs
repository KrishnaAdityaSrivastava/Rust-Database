use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

const MAGIC: &[u8; 4] = b"SST1";
const VERSION: u8 = 1;

// magic        : 4 bytes
// version      : 1 byte
// index_offset : 8 bytes
// index_len    : 8 bytes
// entry_count  : 8 bytes
const HEADER_SIZE: u64 = 29;

#[derive(Debug)]
pub enum Entry {
    Set(String),
    Delete,
}

struct IndexEntry {
    key: String,
    offset: u64,
}

pub struct SSTable {
    file_name: String,
    index: Vec<IndexEntry>,
}

impl SSTable {
    pub fn new(file_name: String) -> io::Result<Self> {
        let mut file = File::create_new(&file_name)?;

        // Reserve space for the header.
        file.write_all(MAGIC)?;
        file.write_all(&[VERSION])?;
        file.write_all(&0u64.to_le_bytes())?; // index_offset
        file.write_all(&0u64.to_le_bytes())?; // index_len
        file.write_all(&0u64.to_le_bytes())?; // entry_count

        file.sync_all()?;

        Ok(SSTable {
            file_name,
            index: Vec::new(),
        })
    }

    pub fn open(file_name: String) -> io::Result<Self> {
        let mut file = File::open(&file_name)?;

        let header = read_header(&mut file)?;

        file.seek(SeekFrom::Start(header.index_offset))?;

        let index = read_index(&mut file, header.index_len, header.entry_count)?;

        Ok(SSTable { file_name, index })
    }

    pub fn write_entries(&mut self, entries: &[(&String, &Entry)]) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).open(&self.file_name)?;

        // Records start immediately after the header.
        file.seek(SeekFrom::Start(HEADER_SIZE))?;

        self.index.clear();

        for (key, entry) in entries {
            let record_offset = file.stream_position()?;

            let key_bytes = key.as_bytes();

            let (entry_type, value_bytes): (u8, &[u8]) = match entry {
                Entry::Set(value) => (0, value.as_bytes()),
                Entry::Delete => (1, &[]),
            };

            let key_len = u32::try_from(key_bytes.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key is too large"))?;

            let value_len = u32::try_from(value_bytes.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "value is too large"))?;

            // Remember where this record starts.
            self.index.push(IndexEntry {
                key: (*key).clone(),
                offset: record_offset,
            });

            // Record:
            //
            // [entry_type : 1 byte]
            // [key_len    : 4 bytes]
            // [value_len  : 4 bytes]
            // [key        : key_len bytes]
            // [value      : value_len bytes]

            file.write_all(&[entry_type])?;
            file.write_all(&key_len.to_le_bytes())?;
            file.write_all(&value_len.to_le_bytes())?;
            file.write_all(key_bytes)?;
            file.write_all(value_bytes)?;
        }

        // Index starts immediately after the records.
        let index_offset = file.stream_position()?;

        for entry in &self.index {
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

        let index_end = file.stream_position()?;
        let index_len = index_end - index_offset;

        let entry_count = u64::try_from(self.index.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many entries"))?;

        // Go back and write the actual header.
        file.seek(SeekFrom::Start(0))?;

        file.write_all(MAGIC)?;
        file.write_all(&[VERSION])?;
        file.write_all(&index_offset.to_le_bytes())?;
        file.write_all(&index_len.to_le_bytes())?;
        file.write_all(&entry_count.to_le_bytes())?;

        file.sync_all()?;

        Ok(())
    }

    pub fn read_entry(&self, key: &str) -> io::Result<Option<Entry>> {
        let index_position = self
            .index
            .binary_search_by(|entry| entry.key.as_str().cmp(key));

        let index_position = match index_position {
            Ok(position) => position,
            Err(_) => return Ok(None),
        };

        let offset = self.index[index_position].offset;

        let mut file = File::open(&self.file_name)?;

        file.seek(SeekFrom::Start(offset))?;

        read_record(&mut file)
    }

    pub fn load_entries(&self) -> io::Result<Vec<(String, Entry)>> {
        let mut file = File::open(&self.file_name)?;

        let mut entries = Vec::with_capacity(self.index.len());

        for index_entry in &self.index {
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

            entries.push((index_entry.key.clone(), entry));
        }

        Ok(entries)
    }
}

struct Header {
    index_offset: u64,
    index_len: u64,
    entry_count: u64,
}

fn read_header(file: &mut File) -> io::Result<Header> {
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;

    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid SSTable magic",
        ));
    }

    let mut version = [0u8; 1];
    file.read_exact(&mut version)?;

    if version[0] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported SSTable version",
        ));
    }

    let index_offset = read_u64(file)?;
    let index_len = read_u64(file)?;
    let entry_count = read_u64(file)?;

    Ok(Header {
        index_offset,
        index_len,
        entry_count,
    })
}

fn read_index(file: &mut File, index_len: u64, entry_count: u64) -> io::Result<Vec<IndexEntry>> {
    let start = file.stream_position()?;
    let end = start
        .checked_add(index_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid index length"))?;

    let capacity = usize::try_from(entry_count)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many index entries"))?;

    let mut index = Vec::with_capacity(capacity);

    for _ in 0..entry_count {
        let key_len = read_u32(file)?;

        let mut key_bytes = vec![0u8; key_len as usize];
        file.read_exact(&mut key_bytes)?;

        let key = String::from_utf8(key_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid UTF-8 key in SSTable index",
            )
        })?;

        let offset = read_u64(file)?;

        index.push(IndexEntry { key, offset });
    }

    let current_position = file.stream_position()?;

    if current_position != end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SSTable index length mismatch",
        ));
    }

    Ok(index)
}

fn read_record(file: &mut File) -> io::Result<Option<Entry>> {
    let mut entry_type = [0u8; 1];

    match file.read_exact(&mut entry_type) {
        Ok(()) => {}

        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(None);
        }

        Err(error) => {
            return Err(error);
        }
    }

    let key_len = read_u32(file)?;
    let value_len = read_u32(file)?;

    let mut key = vec![0u8; key_len as usize];
    file.read_exact(&mut key)?;

    let mut value = vec![0u8; value_len as usize];
    file.read_exact(&mut value)?;

    // We don't currently need the key here because the index
    // already tells us which record we're reading.
    String::from_utf8(key)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 key in SSTable"))?;

    match entry_type[0] {
        0 => {
            let value = String::from_utf8(value).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 value in SSTable")
            })?;

            Ok(Some(Entry::Set(value)))
        }

        1 => {
            if value_len != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "delete record contains a value",
                ));
            }

            Ok(Some(Entry::Delete))
        }

        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid SSTable entry type",
        )),
    }
}

fn read_u32(file: &mut File) -> io::Result<u32> {
    let mut buffer = [0u8; 4];

    file.read_exact(&mut buffer)?;

    Ok(u32::from_le_bytes(buffer))
}

fn read_u64(file: &mut File) -> io::Result<u64> {
    let mut buffer = [0u8; 8];

    file.read_exact(&mut buffer)?;

    Ok(u64::from_le_bytes(buffer))
}
