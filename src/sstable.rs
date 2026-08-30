use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};

pub struct SSTable {
    file_name: String,
}

impl SSTable {
    pub fn new(file_name: String) -> io::Result<Self> {
        File::create(&file_name)?;

        Ok(SSTable { file_name})
    }

    pub fn write_entries(&mut self, map: &HashMap<String, String>) -> io::Result<()> {

        let mut sorted_vec: Vec<(&String, &String)> = map.iter().collect();

        sorted_vec.sort_by(|a, b| a.0.cmp(b.0));
        Ok(())
    }

    pub fn read_entry(&self, key: &str) -> io::Result<Option<String>> {
        let mut content = String::new();

        let mut file = File::open(&self.file_name)?;
        file.read_to_string(&mut content)?;

        for line in content.lines() {
            if let Some((stored_key, value)) = line.split_once(':') {
                if stored_key == key {
                    return Ok(Some(value.to_string()));
                }
            }
        }

        Ok(None)
    }
}