mod bloom;
mod format;
mod index;
mod reader;
mod writer;

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub use format::Entry;

use crate::lsm::sstable::bloom::BloomFilter;

pub struct SSTable {
    file_name: PathBuf,
    file: Mutex<File>,
    index: index::Index,
    bloom_filter: BloomFilter,
}

impl SSTable {
    pub fn new(file_name: PathBuf) -> io::Result<Self> {
        writer::create(&file_name)?;

        let file = File::open(&file_name)?;

        Ok(Self {
            file_name,
            file: Mutex::new(file),
            index: index::Index::new(),
            bloom_filter: BloomFilter::new(1, 0.01),
        })
    }

    pub(crate) fn from_parts(file_name: PathBuf, index: index::Index) -> io::Result<Self> {
        let bloom_filter = reader::load_bloom_filter(&file_name)?;
        let file = File::open(&file_name)?;

        Ok(Self {
            file_name,
            file: Mutex::new(file),
            index,
            bloom_filter,
        })
    }

    pub fn open(file_name: PathBuf) -> io::Result<Self> {
        let bloom_filter = reader::load_bloom_filter(&file_name)?;
        let index = reader::load_index(&file_name)?;
        let file = File::open(&file_name)?;

        Ok(Self {
            file_name,
            file: Mutex::new(file),
            index,
            bloom_filter,
        })
    }

    pub fn write_entries(&mut self, entries: &[(&String, &Entry)]) -> io::Result<()> {
        self.index = writer::write(&self.file_name, entries)?;
        self.bloom_filter = reader::load_bloom_filter(&self.file_name)?;
        
        Ok(())
    }

    pub fn read_entry(&self, key: &str) -> io::Result<Option<Entry>> {
        if !self.bloom_filter.may_contain(key) {
            return Ok(None);
        }

        reader::read_entry(&self.file, &self.index, key)
    }

    pub fn load_entries(&self) -> io::Result<HashMap<String, Entry>> {
        let entries = reader::load_entries_sequential(&self.file_name)?;
        Ok(entries.into_iter().collect())
    }

    pub fn path(&self) -> &Path {
        &self.file_name
    }

    pub(crate) fn sequential_reader(&self) -> io::Result<reader::SequentialReader> {
        reader::SequentialReader::open(&self.file_name)
    }

    pub(crate) fn streaming_writer(path: &Path) -> io::Result<writer::StreamingWriter> {
        writer::StreamingWriter::create(path)
    }
}
