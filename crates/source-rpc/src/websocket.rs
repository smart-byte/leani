//! A receive-only WebSocket client (RFC 6455) with permessage-deflate
//! (RFC 7692), which some sequencer feed relays require and tungstenite does
//! not speak.

use std::{
    fmt::Write as _,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use flate2::{Decompress, FlushDecompress};
use rustls::pki_types::ServerName;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use url::Url;

/// The largest message a feed sends, compressed or not.
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
/// How long connecting, TLS, and the HTTP upgrade may take together.
const HANDSHAKE_WITHIN: Duration = Duration::from_secs(30);
/// The longest line of a handshake response, and the most lines it has.
const MAX_HANDSHAKE_LINE_BYTES: u64 = 8 * 1024;
const MAX_HANDSHAKE_LINES: usize = 64;

pub(crate) struct Socket {
    stream: BufReader<TlsStream<TcpStream>>,
    /// The server's compression context, kept across messages.
    inflate: Option<Decompress>,
}

/// The lines of a handshake response up to the empty one that ends it,
/// refused past [`MAX_HANDSHAKE_LINES`] lines or a line longer than
/// [`MAX_HANDSHAKE_LINE_BYTES`].
async fn response_head<R: AsyncBufRead + Unpin>(stream: &mut R) -> Result<Vec<String>, String> {
    let mut head = Vec::new();
    loop {
        let mut line = String::new();
        let read = (&mut *stream)
            .take(MAX_HANDSHAKE_LINE_BYTES)
            .read_line(&mut line)
            .await
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("the server closed during the handshake".to_owned());
        }
        if !line.ends_with('\n') {
            return Err(format!(
                "a handshake line exceeds {MAX_HANDSHAKE_LINE_BYTES} bytes"
            ));
        }
        if line == "\r\n" {
            return Ok(head);
        }
        if head.len() == MAX_HANDSHAKE_LINES {
            return Err(format!(
                "the handshake response exceeds {MAX_HANDSHAKE_LINES} lines"
            ));
        }
        head.push(line.trim_end().to_owned());
    }
}

/// Inflate one permessage-deflate message in the server's `context`.
fn inflate(context: &mut Decompress, mut data: Vec<u8>) -> Result<Vec<u8>, String> {
    // RFC 7692 strips the empty block that ends each message.
    data.extend([0, 0, 0xff, 0xff]);
    let mut input = data.as_slice();
    let mut output = Vec::with_capacity(data.len() * 4);
    loop {
        output.reserve(64 * 1024);
        let (before_in, before_out) = (context.total_in(), output.len());
        context
            .decompress_vec(input, &mut output, FlushDecompress::Sync)
            .map_err(|error| error.to_string())?;
        let consumed = usize::try_from(context.total_in() - before_in).unwrap_or(usize::MAX);
        input = &input[consumed.min(input.len())..];
        if output.len() > MAX_MESSAGE_BYTES {
            return Err("a feed message inflates past 16 MiB".to_owned());
        }
        if input.is_empty() && output.len() < output.capacity() {
            return Ok(output);
        }
        if consumed == 0 && output.len() == before_out {
            return Err("a compressed feed message does not inflate".to_owned());
        }
    }
}

/// Bytes nobody can guess ahead, as the handshake key and frame masks want.
fn unpredictable() -> [u8; 16] {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_le_bytes()
}

impl Socket {
    /// Open the `wss` URL with the extra `headers`, offering
    /// permessage-deflate, within [`HANDSHAKE_WITHIN`].
    pub(crate) async fn connect(
        url: &Url,
        headers: &[(&str, String)],
        tls: Arc<rustls::ClientConfig>,
    ) -> Result<Self, String> {
        tokio::time::timeout(HANDSHAKE_WITHIN, Self::open(url, headers, tls))
            .await
            .map_err(|_| {
                format!("the feed did not complete its handshake within {HANDSHAKE_WITHIN:?}")
            })?
    }

    async fn open(
        url: &Url,
        headers: &[(&str, String)],
        tls: Arc<rustls::ClientConfig>,
    ) -> Result<Self, String> {
        let host = url.host_str().ok_or("the feed URL names no host")?;
        let port = url.port_or_known_default().unwrap_or(443);
        let tcp = TcpStream::connect((host, port))
            .await
            .map_err(|error| error.to_string())?;
        let name = ServerName::try_from(host.to_owned()).map_err(|error| error.to_string())?;
        let mut stream = TlsConnector::from(tls)
            .connect(name, tcp)
            .await
            .map_err(|error| error.to_string())?;
        let target = url.query().map_or_else(
            || url.path().to_owned(),
            |query| format!("{}?{query}", url.path()),
        );
        let mut request = format!(
            "GET {target} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {}\r\n\
             Sec-WebSocket-Extensions: permessage-deflate\r\n",
            STANDARD.encode(unpredictable())
        );
        for (name, value) in headers {
            let _ = write!(request, "{name}: {value}\r\n");
        }
        request.push_str("\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        let mut stream = BufReader::new(stream);
        let head = response_head(&mut stream).await?;
        let status = head.first().map_or("", String::as_str);
        if status.split(' ').nth(1) != Some("101") {
            // A refusal explains itself in its body.
            let mut reason = vec![0; 512];
            let read = stream.read(&mut reason).await.unwrap_or(0);
            return Err(format!(
                "the handshake was refused: {status}: {}",
                String::from_utf8_lossy(&reason[..read]).trim()
            ));
        }
        let deflate = head.iter().any(|line| {
            let line = line.to_ascii_lowercase();
            line.starts_with("sec-websocket-extensions:") && line.contains("permessage-deflate")
        });
        Ok(Self {
            stream,
            inflate: deflate.then(|| Decompress::new(false)),
        })
    }

    /// The next text or binary message, `None` once the server closes.
    pub(crate) async fn next_message(&mut self) -> Result<Option<Vec<u8>>, String> {
        let mut message = Vec::new();
        let mut compressed = false;
        loop {
            let mut head = [0; 2];
            if let Err(error) = self.stream.read_exact(&mut head).await {
                return if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    Ok(None)
                } else {
                    Err(error.to_string())
                };
            }
            let (fin, rsv1, opcode) = (head[0] & 0x80 != 0, head[0] & 0x40 != 0, head[0] & 0x0f);
            if head[1] & 0x80 != 0 {
                return Err("the server masked a frame".to_owned());
            }
            let length = match head[1] & 0x7f {
                126 => u64::from(
                    self.stream
                        .read_u16()
                        .await
                        .map_err(|error| error.to_string())?,
                ),
                127 => self
                    .stream
                    .read_u64()
                    .await
                    .map_err(|error| error.to_string())?,
                length => u64::from(length),
            };
            let length = usize::try_from(length)
                .ok()
                .filter(|length| message.len() + length <= MAX_MESSAGE_BYTES)
                .ok_or("a feed message exceeds 16 MiB")?;
            let mut payload = vec![0; length];
            self.stream
                .read_exact(&mut payload)
                .await
                .map_err(|error| error.to_string())?;
            match opcode {
                0x8 => return Ok(None),
                0x9 => {
                    self.send(0xa, &payload).await?;
                    continue;
                }
                0xa => continue,
                0x1 | 0x2 => {
                    compressed = rsv1;
                    message = payload;
                }
                0x0 => message.extend(payload),
                opcode => return Err(format!("unknown frame opcode {opcode:#x}")),
            }
            if fin {
                break;
            }
        }
        if compressed {
            let context = self
                .inflate
                .as_mut()
                .ok_or("a compressed message without permessage-deflate")?;
            message = inflate(context, message)?;
        }
        Ok(Some(message))
    }

    /// Send a control frame, masked as a client's must be.
    async fn send(&mut self, opcode: u8, payload: &[u8]) -> Result<(), String> {
        let length = u8::try_from(payload.len())
            .ok()
            .filter(|length| *length <= 125)
            .ok_or("a control frame payload exceeds 125 bytes")?;
        let mask: [u8; 4] = unpredictable()[..4].try_into().expect("four bytes");
        let mut frame = vec![0x80 | opcode, 0x80 | length];
        frame.extend(mask);
        frame.extend(
            payload
                .iter()
                .zip(mask.iter().cycle())
                .map(|(byte, key)| byte ^ key),
        );
        self.stream
            .get_mut()
            .write_all(&frame)
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use flate2::{Compress, Compression, FlushCompress};

    use super::*;

    /// A message as a permessage-deflate server sends it: raw deflate,
    /// sync-flushed, without the empty block that ends it.
    fn deflated(context: &mut Compress, text: &[u8]) -> Vec<u8> {
        let mut wire = Vec::with_capacity(text.len() + 64);
        context
            .compress_vec(text, &mut wire, FlushCompress::Sync)
            .unwrap();
        assert!(wire.ends_with(&[0, 0, 0xff, 0xff]));
        wire.truncate(wire.len() - 4);
        wire
    }

    #[tokio::test]
    async fn a_handshake_response_is_read_within_its_bounds() {
        let read =
            |text: Vec<u8>| async move { response_head(&mut BufReader::new(&text[..])).await };
        let accepted =
            read(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n".to_vec())
                .await
                .expect("head");
        assert_eq!(
            accepted,
            ["HTTP/1.1 101 Switching Protocols", "Upgrade: websocket"]
        );
        let mut long = b"HTTP/1.1 101 Switching Protocols\r\nX: ".to_vec();
        long.extend(std::iter::repeat_n(b'a', 64 * 1024));
        assert!(read(long).await.expect_err("long").contains("line exceeds"));
        let many = std::iter::once("HTTP/1.1 101 Switching Protocols\r\n".to_owned())
            .chain((0..1_000).map(|index| format!("X-{index}: a\r\n")))
            .collect::<String>();
        assert!(
            read(many.into_bytes())
                .await
                .expect_err("many")
                .contains("lines")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_server_that_never_answers_ends_the_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _held = listener.accept().await;
            std::future::pending::<()>().await;
        });
        let url: Url = format!("wss://127.0.0.1:{port}/feed").parse().unwrap();
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        let error = Socket::connect(&url, &[], Arc::new(tls))
            .await
            .err()
            .expect("deadline");
        assert!(error.contains("within"), "{error}");
    }

    #[test]
    fn messages_inflate_in_the_servers_context() {
        let mut server = Compress::new(Compression::default(), false);
        let mut client = Decompress::new(false);
        // The second message refers back into the first.
        for text in [
            &br#"{"version":1,"messages":[{"sequenceNumber":1}]}"#[..],
            &br#"{"version":1,"messages":[{"sequenceNumber":2}]}"#[..],
        ] {
            let wire = deflated(&mut server, text);
            assert_eq!(inflate(&mut client, wire).unwrap(), text);
        }
        assert!(inflate(&mut Decompress::new(false), vec![0xff; 16]).is_err());
    }
}
