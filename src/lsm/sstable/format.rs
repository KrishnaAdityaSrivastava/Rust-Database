use std::io::{self, Read, Write};

pub const MAGIC: &[u8; 4] = b"SST1";
pub const VERSION: u8 = 1;
pub const HEADER_SIZE: u64 = 29;

#[derive(Debug)]
pub enum Entry {
    Set(String),
    Delete,
}

pub struct Header {
    pub index_offset: u64,
    pub index_len: u64,
    pub entry_count: u64,
}

pub fn write_header<W: Write>(writer: &mut W, header: &Header) -> io::Result<()> {
    writer.write_all(MAGIC)?;
    writer.write_all(&[VERSION])?;
    writer.write_all(&header.index_offset.to_le_bytes())?;
    writer.write_all(&header.index_len.to_le_bytes())?;
    writer.write_all(&header.entry_count.to_le_bytes())?;

    Ok(())
}

pub fn read_header<R: Read>(reader: &mut R) -> io::Result<Header> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;

    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid SSTable magic",
        ));
    }

    let mut version = [0u8; 1];
    reader.read_exact(&mut version)?;

    if version[0] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported SSTable version",
        ));
    }

    let mut buffer = [0u8; 8];

    reader.read_exact(&mut buffer)?;
    let index_offset = u64::from_le_bytes(buffer);

    reader.read_exact(&mut buffer)?;
    let index_len = u64::from_le_bytes(buffer);

    reader.read_exact(&mut buffer)?;
    let entry_count = u64::from_le_bytes(buffer);

    Ok(Header {
        index_offset,
        index_len,
        entry_count,
    })
}

pub fn write_record<W: Write>(writer: &mut W, key: &str, entry: &Entry) -> io::Result<()> {
    let key_bytes = key.as_bytes();

    let (entry_type, value_bytes) = match entry {
        Entry::Set(value) => (0u8, value.as_bytes()),
        Entry::Delete => (1u8, &[][..]),
    };

    let key_len = u32::try_from(key_bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key is too large"))?;

    let value_len = u32::try_from(value_bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "value is too large"))?;

    // [entry_type: 1] [key_len: 4] [value_len: 4] [key] [value]

    let mut header = [0u8; 9];

    header[0] = entry_type;
    header[1..5].copy_from_slice(&key_len.to_le_bytes());
    header[5..9].copy_from_slice(&value_len.to_le_bytes());

    writer.write_all(&header)?;
    writer.write_all(key_bytes)?;
    writer.write_all(value_bytes)?;

    Ok(())
}

pub fn read_record<R: Read>(reader: &mut R) -> io::Result<Option<(String, Entry)>> {
    let mut header = [0u8; 9];

    match reader.read_exact(&mut header) {
        Ok(()) => {}

        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(None);
        }

        Err(error) => return Err(error),
    }

    let entry_type = header[0];

    let key_len = u32::from_le_bytes(header[1..5].try_into().unwrap()) as usize;

    let value_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;

    let mut key_bytes = vec![0u8; key_len];
    reader.read_exact(&mut key_bytes)?;

    let key = String::from_utf8(key_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 key in SSTable"))?;

    let entry = match entry_type {
        0 => {
            let mut value_bytes = vec![0u8; value_len];
            reader.read_exact(&mut value_bytes)?;

            let value = String::from_utf8(value_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 value in SSTable")
            })?;

            Entry::Set(value)
        }

        1 => {
            if value_len != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "delete record contains a value",
                ));
            }

            Entry::Delete
        }

        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid SSTable entry type",
            ));
        }
    };

    Ok(Some((key, entry)))
}
