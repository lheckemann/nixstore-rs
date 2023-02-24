use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
//use thiserror::

pub struct NixStoreConnection<T>
where
    T: Read + Write,
{
    connection: T,
}

#[derive(Debug)]
pub enum Error {
    Connect(std::io::Error),
    Read(std::io::Error),
    Write(std::io::Error),
    Flush(std::io::Error),
    ProtocolMismatch,
    Unimplemented,
}

type Result<T> = std::result::Result<T, Error>;

impl NixStoreConnection<UnixStream> {
    pub fn connect_local() -> Result<Self> {
        let path = env::var("NIX_DAEMON_SOCKET_PATH")
            .unwrap_or("/nix/var/nix/daemon-socket/socket".into());
        let stream = UnixStream::connect(path).map_err(Error::Connect)?;
        Self::connect(stream)
    }
}

const WORKER_MAGIC_1: u64 = 0x6e697863;
const WORKER_MAGIC_2: u64 = 0x6478696f;

const STDERR_NEXT: u64 = 0x6f6c6d67;
const STDERR_READ: u64 = 0x64617461;
const STDERR_WRITE: u64 = 0x64617416;
const STDERR_LAST: u64 = 0x616c7473;
const STDERR_ERROR: u64 = 0x63787470;
const STDERR_START_ACTIVITY: u64 = 0x53545254;
const STDERR_STOP_ACTIVITY: u64 = 0x53544f50;
const STDERR_RESULT: u64 = 0x52534c54;

impl<T> NixStoreConnection<T>
where
    T: Read + Write,
{
    fn read_u64(&mut self) -> Result<u64> {
        self.connection
            .read_u64::<LittleEndian>()
            .map_err(Error::Read)
    }

    fn write_u64(&mut self, value: u64) -> Result<()> {
        self.connection
            .write_u64::<LittleEndian>(value)
            .map_err(Error::Write)
    }

    fn init(&mut self) -> Result<()> {
        self.connection
            .write_u64::<LittleEndian>(WORKER_MAGIC_1)
            .map_err(Error::Read)?;
        if self
            .connection
            .read_u64::<LittleEndian>()
            .map_err(Error::Write)?
            != WORKER_MAGIC_2
        {
            return Err(Error::ProtocolMismatch);
        }
        Ok(())
    }

    fn read_string(&mut self) -> Result<String> {
        Err(Error::Unimplemented)
    }
    fn write_string(&mut self, str: &str) -> Result<()> {
        Err(Error::Unimplemented)
    }

    fn process_stderr(&mut self) -> Result<()> {
        self.connection.flush().map_err(Error::Flush)?;
        match self.read_u64()? {
            STDERR_WRITE => {
                unimplemented!()
            }
            STDERR_READ => {
                unimplemented!()
            }
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn connect(connection: T) -> Result<Self> {
        let mut result = Self { connection };
        result.init()?;
        Ok(result)
    }

    pub fn is_valid_path(&mut self, path: &str) -> Result<bool> {
        self.write_u64(1)?; // wopIsValidPath
        self.write_string(&path)?;
        self.process_stderr()?;
        let result = self.read_u64()?;
        return Ok(result != 0);
    }
}

fn main() -> Result<()> {
    let mut conn = NixStoreConnection::connect_local()?;
    let path = "foo";
    let is_valid = conn.is_valid_path(path)?;
    println!("{path} is {}valid", if is_valid {""} else {"in"});
    Ok(())
}
