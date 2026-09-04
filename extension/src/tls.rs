//! TLS and mTLS configuration, built once at worker startup from superuser-only GUCs

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};

/// What the broker does about client certificates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuth {
    /// No CA configured: client certificates are neither requested nor accepted.
    None,
    /// Requested and verified if offered, but a client without one still connects. It
    Optional,
    /// Required. A client without a valid certificate does not complete the handshake.
    Required,
}

#[derive(Debug)]
pub struct TlsSetup {
    pub config: Arc<ServerConfig>,
    pub client_auth: ClientAuth,
}

#[derive(Debug)]
pub enum TlsError {
    Io { path: String, err: String },
    NoCertificates(String),
    NoPrivateKey(String),
    Rustls(String),
    /// `tls_client_cert_required` without a CA to verify against would be a setting that
    RequiredWithoutCa,
    /// One of the certificate/key pair set without the other.
    HalfConfigured(&'static str),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Io { path, err } => write!(f, "reading '{path}': {err}"),
            TlsError::NoCertificates(p) => write!(f, "no certificates in '{p}'"),
            TlsError::NoPrivateKey(p) => write!(f, "no private key in '{p}'"),
            TlsError::Rustls(m) => write!(f, "{m}"),
            TlsError::RequiredWithoutCa => {
                write!(f, "tls_client_cert_required needs kafgres.tls_ca_file")
            }
            TlsError::HalfConfigured(missing) => {
                write!(f, "TLS is half-configured: kafgres.{missing} is not set")
            }
        }
    }
}

fn open(path: &str) -> Result<BufReader<File>, TlsError> {
    File::open(path)
        .map(BufReader::new)
        .map_err(|e| TlsError::Io {
            path: path.to_string(),
            err: e.to_string(),
        })
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let mut reader = open(path)?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::Io {
            path: path.to_string(),
            err: e.to_string(),
        })?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates(path.to_string()));
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>, TlsError> {
    let mut reader = open(path)?;
    // Any of the three PEM spellings: refusing PKCS#1 turns a working `openssl` invocation
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| TlsError::Io {
            path: path.to_string(),
            err: e.to_string(),
        })?
        .ok_or_else(|| TlsError::NoPrivateKey(path.to_string()))
}

/// Build the server config, or `Ok(None)` when TLS is not configured at all.
pub fn build(
    cert_file: Option<&str>,
    key_file: Option<&str>,
    ca_file: Option<&str>,
    require_client_cert: bool,
) -> Result<Option<TlsSetup>, TlsError> {
    let (cert_file, key_file) = match (cert_file, key_file) {
        (Some(c), Some(k)) => (c, k),
        (None, None) => return Ok(None),
        // Half-configured is refused rather than quietly downgraded: the operator believes
        (Some(_), None) => return Err(TlsError::HalfConfigured("tls_key_file")),
        (None, Some(_)) => return Err(TlsError::HalfConfigured("tls_cert_file")),
    };

    if require_client_cert && ca_file.is_none() {
        return Err(TlsError::RequiredWithoutCa);
    }

    let certs = load_certs(cert_file)?;
    let key = load_key(key_file)?;

    let (config, client_auth) = match ca_file {
        None => (
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::Rustls(e.to_string()))?,
            ClientAuth::None,
        ),
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            for cert in load_certs(ca)? {
                roots
                    .add(cert)
                    .map_err(|e| TlsError::Rustls(format!("CA '{ca}': {e}")))?;
            }
            let roots = Arc::new(roots);
            let verifier = if require_client_cert {
                WebPkiClientVerifier::builder(roots)
                    .build()
                    .map_err(|e| TlsError::Rustls(e.to_string()))?
            } else {
                // Verified if presented, absent still allowed. The distinction matters
                WebPkiClientVerifier::builder(roots)
                    .allow_unauthenticated()
                    .build()
                    .map_err(|e| TlsError::Rustls(e.to_string()))?
            };
            (
                ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)
                    .map_err(|e| TlsError::Rustls(e.to_string()))?,
                if require_client_cert {
                    ClientAuth::Required
                } else {
                    ClientAuth::Optional
                },
            )
        }
    };

    Ok(Some(TlsSetup {
        config: Arc::new(config),
        client_auth,
    }))
}

/// The principal for an mTLS connection: the client certificate's subject DN, in RFC 2253
pub fn principal_from_cert(der: &[u8]) -> Option<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).ok()?;
    let dn = cert.subject().to_string();
    // A SAN-only certificate has an empty subject: authenticating it would put an empty
    if dn.is_empty() {
        return None;
    }
    Some(dn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_is_off_only_when_neither_cert_nor_key_is_named() {
        assert!(build(None, None, None, false).unwrap().is_none());
    }

    #[test]
    fn half_configured_tls_is_refused_rather_than_downgraded() {
        // Serving plaintext when the operator configured TLS is silent and everything
        assert!(matches!(
            build(Some("/nope/cert.pem"), None, None, false),
            Err(TlsError::HalfConfigured("tls_key_file"))
        ));
        assert!(matches!(
            build(None, Some("/nope/key.pem"), None, false),
            Err(TlsError::HalfConfigured("tls_cert_file"))
        ));
    }

    #[test]
    fn requiring_a_client_cert_without_a_ca_is_refused() {
        // Otherwise the setting reads as enforced and enforces nothing: with no CA to
        let err = build(Some("/nope/cert.pem"), Some("/nope/key.pem"), None, true);
        assert!(matches!(err, Err(TlsError::RequiredWithoutCa)));
    }

    #[test]
    fn a_missing_file_names_itself() {
        // Startup failures here are read by someone holding a path they typed wrong.
        let err = build(Some("/nope/cert.pem"), Some("/nope/key.pem"), None, false);
        match err {
            Err(TlsError::Io { path, .. }) => assert_eq!(path, "/nope/cert.pem"),
            other => panic!("expected an Io error naming the file, got {other:?}"),
        }
    }
}
