use futures::stream::{Stream, StreamExt};
use simon::Arg;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Instant;
use tokio::{fs, net::UdpSocket};

const BUF_SIZE: usize = 2048;

struct Args {
    socket_addr: SocketAddr,
    directory: String,
    rename: Option<String>,
}

impl Args {
    fn arg() -> impl simon::Arg<Item = Self> {
        simon::args_map! {
            let {
                socket_addr_v4 = simon::opt("a", "address", "serve on address", "HOST:PORT")
                    .with_default_lazy(|| SocketAddrV4::new(Ipv4Addr::new(0,0,0,0), 69));
                directory = simon::free().vec_singleton().choice(
                    simon::opt("d", "directory", "serve files out of directory", "DIR")
                ).with_default_lazy(|| ".".to_string());
                rename = simon::opt("r", "rename", "interpret requests for all files as requests for this file", "FILENAME");
            } in {
                Self {
                    socket_addr: SocketAddr::V4(socket_addr_v4),
                    directory: directory.to_string(),
                    rename,
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

async fn run_server(socket_addr: SocketAddr, directory: &str, rename: Option<&str>) {
    MainSocket {
        socket: UdpSocket::bind(socket_addr).await.unwrap(),
        buf: vec![0; BUF_SIZE],
    }
    .into_stream()
    .await
    .for_each_concurrent(None, |(client_addr, rrq_result)| async move {
        match rrq_result {
            Ok(rrq) => match handle_rrq(socket_addr, directory, rename, client_addr, rrq).await {
                Ok(()) => (),
                Err(FileNotFound) => {
                    log::warn!("file not found");
                    handle_error(socket_addr, client_addr, ResponseError::FileNotFound).await;
                }
            },
            Err(MainSocketError::ParseError) => {
                log::warn!("failed to parse request");
                handle_error(socket_addr, client_addr, ResponseError::ParseError).await;
            }
            Err(MainSocketError::WriteIsNotImplemented) => {
                log::warn!("write is not implemented");
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
    rename: Option<&str>,
    client_addr: ClientAddr,
    rrq: Rrq,
) -> Result<(), FileNotFound> {
    use tokio::io::AsyncReadExt;
    const BLOCK_SIZE: usize = 512;
    const DATA_OFFSET: usize = 4;
    const OPCODE: u8 = 3;
    let start_time = Instant::now();
    log::info!(
        "Handling read request for {} from {}",
        rrq.filename,
        client_addr.0
    );
    let filename_str = if let Some(rename) = rename {
        log::info!("Sending file \"{}\" instead", rename);
        rename
    } else {
        rrq.filename.as_str()
    };
    let path = std::path::Path::new(directory).join(filename_str);
    let mut file = fs::File::open(&path).await.map_err(|_| FileNotFound)?;
    let mut ephemeral_socket = make_ephemeral_socket(socket_addr).await;
    let mut read_into_buf = vec![0u8; BLOCK_SIZE + DATA_OFFSET];
    read_into_buf[0] = 0;
    read_into_buf[1] = OPCODE;
    read_into_buf[2] = 0;
    read_into_buf[3] = 0;
    let mut num_bytes_read = file.read(&mut read_into_buf[DATA_OFFSET..]).await.unwrap();
    let mut send_from_buf = read_into_buf.clone();
    let mut block_num = 0u16;
    loop {
        block_num += 1;
        let next_read_into_buf_fut = file.read(&mut read_into_buf[DATA_OFFSET..]);
        let send_data_fut = send_data(
            block_num,
            &mut send_from_buf[0..(DATA_OFFSET + num_bytes_read)],
            &mut ephemeral_socket,
            client_addr,
        );
        let (num_bytes_read_result, num_bytes_sent) =
            tokio::join!(next_read_into_buf_fut, send_data_fut);
        if num_bytes_sent != read_into_buf.len() {
            break;
        }
        num_bytes_read = num_bytes_read_result.unwrap();
        std::mem::swap(&mut read_into_buf, &mut send_from_buf);
    }
    log::info!(
        "Finished sending {} to {} after {:?}",
        rrq.filename,
        client_addr.0,
        start_time.elapsed()
    );
    Ok(())
}

async fn send_data(
    block_num: u16,
    send_from_buf: &mut [u8],
    socket: &mut UdpSocket,
    client_addr: ClientAddr,
) -> usize {
    send_from_buf[2] = (block_num >> 8) as u8;
    send_from_buf[3] = block_num as u8;
    let num_bytes_sent = socket.send_to(send_from_buf, client_addr.0).await.unwrap();
    let mut ack_buf = [0; 4];
    socket.recv(&mut ack_buf).await.unwrap();
    assert_eq!(&ack_buf[0..2], &[0, 4]);
    assert_eq!(&send_from_buf[2..4], &ack_buf[2..4]);
    num_bytes_sent
}

#[tokio::main]
async fn main() {
    let Args {
        socket_addr,
        directory,
        rename,
    } = Args::arg().with_help_default().parse_env_or_exit();
    env_logger::init();
    run_server(
        socket_addr,
        directory.as_str(),
        rename.as_ref().map(|s| s.as_str()),
    )
    .await;
}
