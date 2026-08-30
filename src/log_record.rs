use std::io::{self, Read, Write};

pub struct LogRecord {
    id: u64,
    command: Command,
    data_type: DataType,
    key: String,
    key_len: u32,
    value: Option<String>,
    value_len: u32,
    checksum: u8,
}

#[derive(Debug, Clone, Copy)]
pub enum Command {
    Set,
    Delete,
}

#[derive(Debug, Clone, Copy)]
pub enum DataType {
    Int,
    Float,
    String,
}

impl LogRecord {
    pub fn new(
        id: u64,
        command: Command,
        data_type: DataType,
        key: String,
        value: Option<String>,
    ) -> Self {
        let key_len = key.len() as u32;

        let value_len = match &value {
            Some(value) => value.len() as u32,
            None => 0,
        };

        let mut record = LogRecord {
            id,
            command,
            data_type,
            key,
            key_len,
            value,
            value_len,
            checksum: 0,
        };
        record.checksum = record.calculate_checksum();

        record
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.id.to_le_bytes())?;

        let command = match self.command {
            Command::Set => 1u8,
            Command::Delete => 2u8,
        };

        writer.write_all(&[command])?;

        let data_type = match self.data_type {
            DataType::Int => 1u8,
            DataType::Float => 2u8,
            DataType::String => 3u8,
        };

        writer.write_all(&[data_type])?;

        writer.write_all(&self.key_len.to_le_bytes())?;
        writer.write_all(&self.value_len.to_le_bytes())?;

        writer.write_all(self.key.as_bytes())?;

        if let Some(value) = &self.value {
            writer.write_all(value.as_bytes())?;
        }

        writer.write_all(&[self.checksum])?;

        Ok(())
    }

    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        let mut id_bytes = [0u8; 8];

        match reader.read_exact(&mut id_bytes) {
            Ok(()) => {}

            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }

            Err(e) => return Err(e),
        }

        let id = u64::from_le_bytes(id_bytes);

        let mut byte = [0u8; 1];

        reader.read_exact(&mut byte)?;

        let command = match byte[0] {
            1 => Command::Set,
            2 => Command::Delete,

            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid command",
                ));
            }
        };

        reader.read_exact(&mut byte)?;

        let data_type = match byte[0] {
            1 => DataType::Int,
            2 => DataType::Float,
            3 => DataType::String,

            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid data type",
                ));
            }
        };

        let mut buf = [0u8; 4];

        reader.read_exact(&mut buf)?;
        let key_len = u32::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let value_len = u32::from_le_bytes(buf);

        let mut key_bytes = vec![0u8; key_len as usize];

        reader.read_exact(&mut key_bytes)?;

        let key = String::from_utf8(key_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 key"))?;

        let value = match command {
            Command::Set => {
                let mut value_bytes = vec![0u8; value_len as usize];

                reader.read_exact(&mut value_bytes)?;

                Some(String::from_utf8(value_bytes).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 value")
                })?)
            }

            Command::Delete => None,
        };

        reader.read_exact(&mut byte)?;

        let checksum = byte[0];

        let record = Self {
            id,
            command,
            data_type,
            key,
            key_len,
            value,
            value_len,
            checksum,
        };

        // Validate checksum.
        if record.calculate_checksum() != record.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }

        Ok(Some(record))
    }

    pub fn calculate_checksum(&self) -> u8 {
        let mut checksum = 0u8;

        // ID
        for byte in self.id.to_le_bytes() {
            checksum ^= byte;
        }

        // Command
        checksum ^= match self.command {
            Command::Set => 1,
            Command::Delete => 2,
        };

        // Data type
        checksum ^= match self.data_type {
            DataType::Int => 1,
            DataType::Float => 2,
            DataType::String => 3,
        };

        // Key length
        for byte in self.key_len.to_le_bytes() {
            checksum ^= byte;
        }

        // Value length
        for byte in self.value_len.to_le_bytes() {
            checksum ^= byte;
        }

        // Key
        for byte in self.key.as_bytes() {
            checksum ^= byte;
        }

        // Value
        if let Some(value) = &self.value {
            for byte in value.as_bytes() {
                checksum ^= byte;
            }
        }

        checksum
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn command(&self) -> Command {
        self.command
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    pub fn checksum(&self) -> u8 {
        self.checksum
    }
}
