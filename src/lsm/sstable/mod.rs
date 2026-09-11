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

pub struct SSTable {
    file_name: PathBuf,
    file: Mutex<File>,
    index: index::Index,
}

impl SSTable {
    pub fn new(file_name: PathBuf) -> io::Result<Self> {
        writer::create(&file_name)?;

        let file = File::open(&file_name)?;

        Ok(Self {
            file_name,
            file: Mutex::new(file),
            index: index::Index::new(),
        })
    }

    pub(crate) fn from_parts(file_name: PathBuf, index: index::Index) -> io::Result<Self> {
        let file = File::open(&file_name)?;

        Ok(Self {
            file_name,
            file: Mutex::new(file),
            index,
        })
    }

    pub fn open(file_name: PathBuf) -> io::Result<Self> {
        let index = reader::load_index(&file_name)?;
        let file = File::open(&file_name)?;

        Ok(Self {
            file_name,
            file: Mutex::new(file),
            index,
        })
    }

    pub fn write_entries(&mut self, entries: &[(&String, &Entry)]) -> io::Result<()> {
        self.index = writer::write(&self.file_name, entries)?;

        Ok(())
    }

    pub fn read_entry(&self, key: &str) -> io::Result<Option<Entry>> {
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
