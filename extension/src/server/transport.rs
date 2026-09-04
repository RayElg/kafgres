//! Plaintext or TLS, behind one read/write surface.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

/// The socket, with or without TLS on top.
pub enum Transport {
    Plain(TcpStream),
    Tls {
        sock: TcpStream,
        /// Boxed: `ServerConnection` is large, and a `Conn` lives in a map that moves.
        tls: Box<rustls::ServerConnection>,
    },
}

fn would_block() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::WouldBlock, "no plaintext available")
}

/// `pump`, but on the parts rather than on `self` — the read path needs to flush an
fn self_pump(sock: &mut TcpStream, tls: &mut rustls::ServerConnection) -> bool {
    while tls.wants_write() {
        match tls.write_tls(sock) {
            Ok(0) => return false,
            Ok(_) => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return false,
        }
    }
    true
}

fn tls_failed(e: rustls::Error) -> std::io::Error {
    std::io::Error::other(format!("tls: {e}"))
}

impl Transport {
    pub fn plain(sock: TcpStream) -> Self {
        Transport::Plain(sock)
    }

    pub fn tls(sock: TcpStream, config: Arc<rustls::ServerConfig>) -> std::io::Result<Self> {
        let tls = rustls::ServerConnection::new(config).map_err(tls_failed)?;
        Ok(Transport::Tls {
            sock,
            tls: Box::new(tls),
        })
    }

    pub fn is_tls(&self) -> bool {
        matches!(self, Transport::Tls { .. })
    }

    /// True once the TLS handshake has completed. Always true for plaintext.
    pub fn handshake_done(&self) -> bool {
        match self {
            Transport::Plain(_) => true,
            Transport::Tls { tls, .. } => !tls.is_handshaking(),
        }
    }

    /// The peer's leaf certificate, if it presented one.
    pub fn peer_certificate(&self) -> Option<Vec<u8>> {
        match self {
            Transport::Plain(_) => None,
            // The leaf is first; the rest of the chain is the CA's business, and the
            Transport::Tls { tls, .. } => tls
                .peer_certificates()
                .and_then(|c| c.first())
                .map(|c| c.as_ref().to_vec()),
        }
    }

    pub fn set_nodelay(&self, on: bool) -> std::io::Result<()> {
        self.socket().set_nodelay(on)
    }

    fn socket(&self) -> &TcpStream {
        match self {
            Transport::Plain(s) => s,
            Transport::Tls { sock, .. } => sock,
        }
    }

    /// Push whatever TLS owes the socket. Safe and cheap to call on any tick.
    pub fn pump(&mut self) -> bool {
        let (sock, tls) = match self {
            Transport::Plain(_) => return true,
            Transport::Tls { sock, tls } => (sock, tls),
        };
        self_pump(sock, tls)
    }
}

impl Read for Transport {
    /// Plaintext, or `WouldBlock`.
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let (sock, tls) = match self {
            Transport::Plain(s) => return s.read(buf),
            Transport::Tls { sock, tls } => (sock, tls),
        };

        let mut saw_eof = false;
        while tls.wants_read() {
            match tls.read_tls(sock) {
                Ok(0) => {
                    saw_eof = true;
                    break;
                }
                Ok(_) => {
                    if let Err(e) = tls.process_new_packets() {
                        // rustls has already queued a fatal alert describing *why*, and
                        let _ = self_pump(sock, tls);
                        return Err(tls_failed(e));
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        // Handshake responses have to leave before the peer will send anything else, and
        if !self.pump() {
            return Ok(0);
        }
        let tls = match self {
            Transport::Tls { tls, .. } => tls,
            Transport::Plain(_) => unreachable!("checked above"),
        };

        match tls.reader().read(buf) {
            // rustls returns `Ok(0)` from `reader()` for exactly one reason: the peer
            Ok(0) => Ok(0),
            Ok(n) => Ok(n),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if saw_eof {
                    Ok(0)
                } else {
                    Err(would_block())
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(s) => s.write(buf),
            Transport::Tls { tls, .. } => {
                // Accepted into rustls's buffer, not yet on the wire. Reporting it as
                let n = tls.writer().write(buf)?;
                if !self.pump() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "tls write failed",
                    ));
                }
                if n == 0 {
                    // rustls caps its own plaintext buffer (64 KiB by default) and
                    return Err(would_block());
                }
                Ok(n)
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Transport::Plain(s) => s.flush(),
            Transport::Tls { .. } => {
                self.pump();
                Ok(())
            }
        }
    }
}
