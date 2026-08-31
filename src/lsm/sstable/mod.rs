mod format;
mod index;
mod reader;
mod writer;

use std::io;
use std::path::{Path, PathBuf};

use std::collections::HashMap;

pub use format::Entry;

pub struct SSTable {
    file_name: PathBuf,
    index: index::Index,
}

impl SSTable {
    pub fn new(file_name: PathBuf) -> io::Result<Self> {
        writer::create(&file_name)?;

        Ok(Self {
            file_name,
            index: index::Index::new(),
        })
    }

    pub fn open(file_name: PathBuf) -> io::Result<Self> {
        let index = reader::load_index(&file_name)?;

        Ok(Self { file_name, index })
    }

    pub fn write_entries(&mut self, entries: &[(&String, &Entry)]) -> io::Result<()> {
        self.index = writer::write(&self.file_name, entries)?;

        Ok(())
    }

    pub fn read_entry(&self, key: &str) -> io::Result<Option<Entry>> {
        reader::read_entry(&self.file_name, &self.index, key)
    }

    pub fn load_entries(&self) -> io::Result<HashMap<String, Entry>> {
        reader::load_entries(&self.file_name, &self.index)
    }

    pub fn path(&self) -> &Path {
        &self.file_name
    }
}
