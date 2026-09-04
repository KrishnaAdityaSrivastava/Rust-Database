enum Command {
    Set,
    Get,
    Delete,
}

struct Request {
    command: Command,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

enum Status {
    OK,
    NOT_FOUND,
    ERROR,
}

struct Response {
    status: Status,
    value: Option<Vec<u8>>,
}

impl Command {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Command::Set),
            2 => Some(Command::Get),
            3 => Some(Command::Delete),
            _ => None,
        }
    }

    fn to_u8(&self) -> u8 {
        match self {
            Command::Set => 1,
            Command::Get => 2,
            Command::Delete => 3,
        }
    }
}

