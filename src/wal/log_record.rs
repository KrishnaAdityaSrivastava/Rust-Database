use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    id: u64,
    pub(crate) command: Command,
    checksum: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: Value },
    Delete { key: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Float,
    String,
}

impl DataType {
    pub fn code(self) -> u8 {
        match self {
            Self::Int => 1,
            Self::Float => 2,
            Self::String => 3,
        }
    }

    pub fn from_code(code: u8) -> io::Result<Self> {
        match code {
            1 => Ok(Self::Int),
            2 => Ok(Self::Float),
            3 => Ok(Self::String),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid data type",
            )),
        }
    }
    pub fn from_u8(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(Self::Int),
            1 => Ok(Self::Float),
            2 => Ok(Self::String),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid SSTable value type",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Int(_) => DataType::Int,
            Self::Float(_) => DataType::Float,
            Self::String(_) => DataType::String,
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::Int(value) => value.to_string().into_bytes(),
            Self::Float(value) => value.to_string().into_bytes(),
            Self::String(value) => value.as_bytes().to_vec(),
        }
    }
    pub fn from_bytes(data_type: DataType, bytes: Vec<u8>) -> io::Result<Self> {
        match data_type {
            DataType::Int => {
                let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid integer value length")
                })?;

                Ok(Self::Int(i64::from_le_bytes(bytes)))
            }

            DataType::Float => {
                let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid float value length")
                })?;

                Ok(Self::Float(f64::from_le_bytes(bytes)))
            }

            DataType::String => String::from_utf8(bytes).map(Self::String).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 string value")
            }),
        }
    }
}

impl Command {
    fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Delete { key } => key,
        }
    }

    fn key_len(&self) -> u32 {
        self.key().len() as u32
    }

    fn value_len(&self) -> u32 {
        match self {
            Self::Set { value, .. } => value.as_bytes().len() as u32,
            Self::Delete { .. } => 0,
        }
    }

    fn value(&self) -> Option<&Value> {
        match self {
            Self::Set { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }

    fn data_type(&self) -> Option<DataType> {
        self.value().map(Value::data_type)
    }

    fn command_code(&self) -> u8 {
        match self {
            Self::Set { .. } => 1,
            Self::Delete { .. } => 2,
        }
    }
}

impl LogRecord {
    pub fn new(id: u64, command: Command) -> Self {
        let mut record = Self {
            id,
            command,
            checksum: 0,
        };

        record.checksum = record.calculate_checksum();
        record
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.id.to_le_bytes())?;
        writer.write_all(&[self.command.command_code()])?;

        match &self.command {
            Command::Set { key, value } => {
                let value_bytes = value.as_bytes();

                writer.write_all(&[value.data_type().code()])?;
                writer.write_all(&(key.len() as u32).to_le_bytes())?;
                writer.write_all(&(value_bytes.len() as u32).to_le_bytes())?;
                writer.write_all(key.as_bytes())?;
                writer.write_all(&value_bytes)?;
            }

            Command::Delete { key } => {
                writer.write_all(&[0])?;
                writer.write_all(&(key.len() as u32).to_le_bytes())?;
                writer.write_all(&0u32.to_le_bytes())?;
                writer.write_all(key.as_bytes())?;
            }
        }

        writer.write_all(&[self.checksum])?;

        Ok(())
    }

    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        let Some(id) = read_u64(reader)? else {
            return Ok(None);
        };

        let command_code = read_u8(reader)?;

        let command = match command_code {
            1 => Self::read_set(reader)?,
            2 => Self::read_delete(reader)?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid command",
                ));
            }
        };

        let checksum = read_u8(reader)?;

        let record = Self {
            id,
            command,
            checksum,
        };

        if record.calculate_checksum() != checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }

        Ok(Some(record))
    }

    fn read_set<R: Read>(reader: &mut R) -> io::Result<Command> {
        let data_type = DataType::from_code(read_u8(reader)?)?;

        let key_len = read_u32(reader)?;
        let value_len = read_u32(reader)?;

        let key = read_string(reader, key_len, "key")?;
        let value_bytes = read_bytes(reader, value_len)?;

        let value = match data_type {
            DataType::Int => {
                let value = read_utf8(value_bytes, "value")?;

                Value::Int(value.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid integer value")
                })?)
            }

            DataType::Float => {
                let value = read_utf8(value_bytes, "value")?;

                Value::Float(value.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid float value")
                })?)
            }

            DataType::String => Value::String(read_utf8(value_bytes, "value")?),
        };

        Ok(Command::Set { key, value })
    }

    fn read_delete<R: Read>(reader: &mut R) -> io::Result<Command> {
        let data_type = read_u8(reader)?;

        if data_type != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid delete data type",
            ));
        }

        let key_len = read_u32(reader)?;
        let _value_len = read_u32(reader)?;

        let key = read_string(reader, key_len, "key")?;

        Ok(Command::Delete { key })
    }

    pub fn calculate_checksum(&self) -> u8 {
        let mut checksum = 0u8;

        for byte in self.id.to_le_bytes() {
            checksum ^= byte;
        }

        checksum ^= self.command.command_code();

        if let Some(data_type) = self.command.data_type() {
            checksum ^= data_type.code();
        }

        for byte in self.command.key_len().to_le_bytes() {
            checksum ^= byte;
        }

        for byte in self.command.value_len().to_le_bytes() {
            checksum ^= byte;
        }

        for byte in self.command.key().as_bytes() {
            checksum ^= byte;
        }

        if let Some(value) = self.command.value() {
            for byte in value.as_bytes() {
                checksum ^= byte;
            }
        }

        checksum
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn command(&self) -> &Command {
        &self.command
    }

    pub fn data_type(&self) -> Option<DataType> {
        self.command.data_type()
    }

    pub fn key(&self) -> &str {
        self.command.key()
    }

    pub fn value(&self) -> Option<&Value> {
        self.command.value()
    }

    pub fn checksum(&self) -> u8 {
        self.checksum
    }
}

// ---------- Binary reading helpers ----------

fn read_u8<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64<R: Read>(reader: &mut R) -> io::Result<Option<u64>> {
    let mut buf = [0u8; 8];

    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(u64::from_le_bytes(buf))),

        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),

        Err(e) => Err(e),
    }
}

fn read_bytes<R: Read>(reader: &mut R, len: u32) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0u8; len as usize];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_string<R: Read>(reader: &mut R, len: u32, field: &str) -> io::Result<String> {
    let bytes = read_bytes(reader, len)?;

    String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("invalid UTF-8 {field}")))
}

fn read_utf8(bytes: Vec<u8>, field: &str) -> io::Result<String> {
    String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("invalid UTF-8 {field}")))
}
