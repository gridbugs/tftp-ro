use futures::stream::{Stream, StreamExt};
use simon::Arg;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::{fs, net::UdpSocket};

const BUF_SIZE: usize = 2048;

struct Args {
    socket_addr: SocketAddr,
    directory: String,
}

impl Args {
    fn arg() -> impl simon::Arg<Item = Self> {
        simon::args_map! {
            let {
                socket_addr_v4 = simon::opt("a", "address", "serve on address", "HOST:PORT")
                    .with_default_lazy(|| SocketAddrV4::new(Ipv4Addr::new(0,0,0,0), 69));
                directory = simon::opt("d", "directory", "serve files out of directory", "DIR")
                    .with_default_lazy(|| ".".to_string());
            } in {
                Self {
                    socket_addr: SocketAddr::V4(socket_addr_v4),
                    directory: directory.to_string(),
                }
            }
        }
    }
}

#[derive(Debug)]
enum Mode {
    Netascii,
    Octet,
    Mail,
}

#[derive(Debug)]
struct Rrq {
    filename: String,
    mode: Mode,
}

#[derive(Debug)]
enum TftpRequest {
    Rrq(Rrq),
    Wrq,
}

#[derive(Debug)]
enum Opcode {
    Rrq,
    Wrq,
}

struct ParseError;

fn parse_netascii_string(bytes: &[u8]) -> Result<(String, usize), ParseError> {
    let mut invalid_char_in_filename = false;
    let mut count = 0;
    let string = bytes
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| std::char::from_u32(b as u32))
        .inspect(|maybe_char| {
            count += 1;
            if maybe_char.is_none() {
                invalid_char_in_filename = true
            }
        })
        .filter_map(|maybe_char| maybe_char)
        .collect::<String>();
    if invalid_char_in_filename {
        return Err(ParseError);
    }
    Ok((string, count))
}

impl TftpRequest {
    fn parse(bytes: &[u8]) -> Result<Self, ParseError> {
        const OPCODE_SIZE: usize = 2;
        let opcode = match &bytes[0..OPCODE_SIZE] {
            [0, 1] => Opcode::Rrq,
            [0, 2] => Opcode::Wrq,
            _ => return Err(ParseError),
        };
        let (filename, offset) = parse_netascii_string(&bytes[OPCODE_SIZE..])?;
        let (mut mode_string, _) = parse_netascii_string(&bytes[OPCODE_SIZE + offset + 1..])?;
        mode_string.make_ascii_lowercase();
        let mode = match mode_string.as_str() {
            "netascii" => Mode::Netascii,
            "octet" => Mode::Octet,
            "mail" => Mode::Mail,
            _ => return Err(ParseError),
        };
        let request = match opcode {
            Opcode::Rrq => Self::Rrq(Rrq { filename, mode }),
            Opcode::Wrq => Self::Wrq,
        };
        Ok(request)
    }
}

async fn make_ephemeral_socket(socket_addr: SocketAddr) -> UdpSocket {
    let mut ephemeral_addr = socket_addr;
    ephemeral_addr.set_port(0);
    UdpSocket::bind(ephemeral_addr).await.unwrap()
}

enum ResponseError {
    Unimplemented,
    ParseError,
    FileNotFound,
}

impl ResponseError {
    fn encode(self) -> Vec<u8> {
        let (code, message) = match self {
            Self::Unimplemented => (0, "unimplemented"),
            Self::ParseError => (0, "error parsing request"),
            Self::FileNotFound => (1, "file not found"),
        };
        let mut out = vec![0, 5, 0, code];
        out.extend_from_slice(message.as_bytes());
        out.push(0);
        out
    }
}

async fn handle_error(socket_addr: SocketAddr, client_addr: ClientAddr, error: ResponseError) {
    let buf = error.encode();
    let mut ephemeral_socket = make_ephemeral_socket(socket_addr).await;
    ephemeral_socket.send_to(&buf, client_addr.0).await.unwrap();
}

struct MainSocket {
    socket: UdpSocket,
    buf: Vec<u8>,
}

enum MainSocketError {
    WriteIsNotImplemented,
    ParseError,
}

#[derive(Clone, Copy)]
struct ClientAddr(SocketAddr);

impl MainSocket {
    async fn into_stream(self) -> impl Stream<Item = (ClientAddr, Result<Rrq, MainSocketError>)> {
        futures::stream::unfold(self, |mut s| async move {
            let (size, client_addr) = s.socket.recv_from(&mut s.buf).await.unwrap();
            let request = if let Ok(request) = TftpRequest::parse(&s.buf[0..size]) {
                request
            } else {
                return Some((
                    (ClientAddr(client_addr), Err(MainSocketError::ParseError)),
                    s,
                ));
            };
            let rrq = match request {
                TftpRequest::Rrq(rrq) => rrq,
                TftpRequest::Wrq => {
                    return Some((
                        (
                            ClientAddr(client_addr),
                            Err(MainSocketError::WriteIsNotImplemented),
                        ),
                        s,
                    ));
                }
            };
            Some(((ClientAddr(client_addr), Ok(rrq)), s))
        })
    }
}

async fn run_server(socket_addr: SocketAddr, directory: &str) {
    MainSocket {
        socket: UdpSocket::bind(socket_addr).await.unwrap(),
        buf: vec![0; BUF_SIZE],
    }
    .into_stream()
    .await
    .for_each_concurrent(None, |(client_addr, rrq_result)| async move {
        match rrq_result {
            Ok(rrq) => match handle_rrq(socket_addr, directory, client_addr, rrq).await {
                Ok(()) => (),
                Err(FileNotFound) => {
                    println!("file not found");
                    handle_error(socket_addr, client_addr, ResponseError::FileNotFound).await;
                }
            },
            Err(MainSocketError::ParseError) => {
                println!("failed to parse request");
                handle_error(socket_addr, client_addr, ResponseError::ParseError).await;
            }
            Err(MainSocketError::WriteIsNotImplemented) => {
                println!("write is not implemented");
                handle_error(socket_addr, client_addr, ResponseError::Unimplemented).await;
            }
        }
    })
    .await;
}

struct FileNotFound;

async fn handle_rrq(
    socket_addr: SocketAddr,
    directory: &str,
    client_addr: ClientAddr,
    rrq: Rrq,
) -> Result<(), FileNotFound> {
    let path = std::path::Path::new(directory).join(rrq.filename.as_str());
    let file = fs::File::open(&path).await.map_err(|_| FileNotFound)?;
    println!(
        "sending file \"{}\" with mode {:?}",
        path.to_string_lossy(),
        rrq.mode
    );
    send_file(socket_addr, client_addr, file, rrq.mode).await;
    Ok(())
}

async fn send_file(
    socket_addr: SocketAddr,
    client_addr: ClientAddr,
    mut file: fs::File,
    mode: Mode,
) {
    use tokio::io::AsyncReadExt;
    let mut ephemeral_socket = make_ephemeral_socket(socket_addr).await;
    let mut all_data = Vec::new();
    file.read_to_end(&mut all_data).await.unwrap();
    let num_full_chunks = all_data.len() / CHUNK_SIZE;
    let num_chunks = num_full_chunks + 1;
    println!("num_chunks: {}", num_chunks);
    for i in 0..num_chunks {
        let slice = if i < num_full_chunks {
            &all_data[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE]
        } else {
            &all_data[i * CHUNK_SIZE..]
        };
        let buf = data_packet(i as u16 + 1, slice);
        println!("sending chunk {}", i + 1);
        ephemeral_socket.send_to(&buf, client_addr.0).await.unwrap();
        let mut buf = [0; 4];
        ephemeral_socket.recv(&mut buf).await.unwrap();
        println!(
            "received ack for chunk {}",
            (buf[2] as u16) << 8 | buf[3] as u16
        );
    }
}

const CHUNK_SIZE: usize = 512;

fn data_packet(block_num: u16, data: &[u8]) -> Vec<u8> {
    let mut packet = vec![0, 3, (block_num >> 8) as u8, block_num as u8];
    packet.extend_from_slice(data);
    packet
}

async fn chunk_file(mut file: fs::File) -> impl Stream<Item = Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut all_data = Vec::new();
    file.read_to_end(&mut all_data).await.unwrap();
    let num_full_chunks = all_data.len() / CHUNK_SIZE;
    let num_chunks = num_full_chunks + 1;
    println!("num_chunks: {}", num_chunks);
    futures::stream::unfold((1, all_data), move |(block_num, all_data)| async move {
        use std::cmp::Ordering;
        let index = block_num - 1;
        println!("processing {}", index);
        match block_num.cmp(&num_chunks) {
            Ordering::Less => {
                let slice = &all_data[index * CHUNK_SIZE..block_num * CHUNK_SIZE];
                Some((
                    data_packet(block_num as u16, slice),
                    (block_num + 1, all_data),
                ))
            }
            Ordering::Equal => {
                let slice = &all_data[index * CHUNK_SIZE..];
                Some((
                    data_packet(block_num as u16, slice),
                    (block_num + 1, all_data),
                ))
            }
            Ordering::Greater => None,
        }
    })
}

#[tokio::main]
async fn main() {
    let Args {
        socket_addr,
        directory,
    } = Args::arg().with_help_default().parse_env_or_exit();
    run_server(socket_addr, directory.as_str()).await;
}
