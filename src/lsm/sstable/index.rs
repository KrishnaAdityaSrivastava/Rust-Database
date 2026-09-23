use std::io;

pub struct IndexEntry {
    pub key: String,
    pub offset: u64,
}

pub struct Index {
    entries: Vec<IndexEntry>,
}

impl Index {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, key: String, offset: u64) {
        self.entries.push(IndexEntry { key, offset });
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn find(&self, key: &str) -> Option<u64> {
        self.entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .map(|position| self.entries[position].offset)
    }

    pub fn iter(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.iter()
    }
}

pub fn read_index<R: std::io::Read + std::io::Seek>( file: &mut R, index_len: u64, entry_count: u64) -> io::Result<Index> {
    let start = file.stream_position()?;

    let end = start
        .checked_add(index_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid index length"))?;

    let capacity = usize::try_from(entry_count)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many index entries"))?;

    let mut index = Index {
        entries: Vec::with_capacity(capacity),
    };

    for _ in 0..entry_count {
        let mut buffer = [0u8; 4];
        file.read_exact(&mut buffer)?;

        let key_len = u32::from_le_bytes(buffer);

        let mut key_bytes = vec![0u8; key_len as usize];
        file.read_exact(&mut key_bytes)?;

        let key = String::from_utf8(key_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid UTF-8 key in SSTable index",
            )
        })?;

        let mut buffer = [0u8; 8];
        file.read_exact(&mut buffer)?;

        let offset = u64::from_le_bytes(buffer);

        index.add(key, offset);
    }

    if file.stream_position()? != end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SSTable index length mismatch",
        ));
    }

    // binary_search requires sorted keys.
    for pair in index.entries.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SSTable index is not sorted",
            ));
        }
    }

    Ok(index)
}
