use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::{ChildStdin, ChildStdout, Command, Stdio},
    string::FromUtf8Error,
};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
//use thiserror::

pub struct NixStoreConnection<T>
where
    T: Read + Write,
{
    connection: T,
    daemon_version: u64,
    daemon_nix_version: String,
}

#[derive(Debug)]
pub enum Error {
    Connect(std::io::Error),
    Read(std::io::Error),
    Write(std::io::Error),
    Flush(std::io::Error),
    StderrWrite(std::io::Error),
    ParseUTF8(std::string::FromUtf8Error),
    ProtocolMismatch,
    Unimplemented,
    UnsupportedProtocolVersion(u64),
    SpawnChild(std::io::Error),
    UnsupportedFieldType(u64),
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

const PROTOCOL_VERSION: u64 = 0x0100 | 34;

const NULS: [u8; 8] = [0u8; 8];

struct RW<R, W>
where
    R: Read,
    W: Write,
{
    r: R,
    w: W,
}
impl<R, W> Read for RW<R, W>
where
    R: Read,
    W: Write,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.r.read(buf)
    }
}
impl<R, W> Write for RW<R, W>
where
    R: Read,
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.w.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

#[derive(Debug)]
enum Field {
    Int(u64),
    String(String),
}

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
        self.write_u64(WORKER_MAGIC_1)?;
        if self.read_u64()? != WORKER_MAGIC_2 {
            return Err(Error::ProtocolMismatch);
        }
        self.daemon_version = self.read_u64()?;
        // TODO: support other versions
        if self.daemon_version != PROTOCOL_VERSION {
            return Err(Error::UnsupportedProtocolVersion(self.daemon_version));
        }
        self.write_u64(PROTOCOL_VERSION)?;
        self.write_u64(0)?; // obsolete CPU affinity
        self.write_u64(0)?; // obsolete reserveSpace
        self.connection.flush().map_err(Error::Flush)?;
        self.daemon_nix_version = self.read_string()?;
        self.process_stderr()?;
        Ok(())
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u64()? as usize;
        let mut buf = vec![0u8; len];
        self.connection.read_exact(&mut buf).map_err(Error::Read)?;
        let mut padding = vec![0u8; 8 - len % 8];
        self.connection
            .read_exact(&mut padding)
            .map_err(Error::Read)?;
        String::from_utf8(buf).map_err(Error::ParseUTF8)
    }
    fn write_string(&mut self, str: &str) -> Result<()> {
        self.write_u64(str.len() as u64)?;
        self.connection
            .write_all(&str.as_bytes())
            .map_err(Error::Write)?;
        // padding
        self.connection
            .write_all(&NULS[..(8 - str.len() % 8)])
            .map_err(Error::Write)?;
        Ok(())
    }

    fn read_fields(&mut self) -> Result<Vec<Field>> {
        let num_fields = self.read_u64()?;
        let mut result = Vec::with_capacity(num_fields as usize);
        for _ in 0..num_fields {
            let field_type = self.read_u64()?;
            result.push(match field_type {
                0 => {
                    // tInt
                    Field::Int(self.read_u64()?)
                }
                1 => {
                    // tString
                    Field::String(self.read_string()?)
                }
                _ => return Err(Error::UnsupportedFieldType(field_type)),
            });
        }
        Ok(result)
    }

    fn process_stderr(&mut self) -> Result<()> {
        // TODO: make flushing optional? It is in Nix
        self.connection.flush().map_err(Error::Flush)?;

        loop {
            match self.read_u64()? {
                STDERR_WRITE => {
                    let s = self.read_string()?;
                    // TODO: allow replacing stderr
                    std::io::stderr()
                        .write_all(&s.as_bytes())
                        .map_err(Error::StderrWrite)?;
                }
                STDERR_START_ACTIVITY => {
                    let activity_id = self.read_u64()?;
                    let level = self.read_u64()?;
                    let activity_type = self.read_u64()?;
                    let description = self.read_string()?;
                    let fields = self.read_fields()?;
                    let parent_activity_id = self.read_u64()?;

                    eprintln!(
                        "START_ACTIVITY
  id: {activity_id}
  level: {level}
  type: {activity_type}
  description: {description}
  fields: {fields:?}
  parent_activity_id: {parent_activity_id}"
                    );
                }
                STDERR_LAST => {
                    break;
                }
                n => {
                    panic!("Unimplemented stderr message: {n:#x}");
                }
            }
        }
        Ok(())
    }

    pub fn connect(connection: T) -> Result<Self> {
        let mut result = Self {
            connection,
            daemon_version: 0,
            daemon_nix_version: String::from(""),
        };
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

impl NixStoreConnection<RW<ChildStdout, ChildStdin>> {
    pub fn connect_to_store(uri: &str) -> Result<Self> {
        let mut command = if true {
            let mut command = Command::new("gdbserver");
            command.arg("localhost:1234").arg("nix-daemon");
            command
        } else {
            Command::new("nix-daemon")
        };
        command
            .arg("--store")
            .arg(uri)
            .arg("--stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let process = command
            .spawn()
            .map_err(Error::SpawnChild)?;
        Self::connect(RW {
            r: process.stdout.unwrap(),
            w: process.stdin.unwrap(),
        })
    }
}

fn main() -> Result<()> {
    //let mut conn = NixStoreConnection::connect_local()?;
    let mut conn = NixStoreConnection::connect_to_store("https://cache.nixos.org")?;
    let path =
        "/nix/store/zw1yqigr88q180q8lgql3zx9yq6z33zk-nixos-system-geruest-22.11-20230207-af96094";
    let is_valid = conn.is_valid_path(path)?;
    println!("{path} is {}valid", if is_valid { "" } else { "in" });
    Ok(())
}
