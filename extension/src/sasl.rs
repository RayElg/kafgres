//! SCRAM-SHA-256 against Postgres roles: `pg_authid.rolpassword` already holds the

use base64::Engine;
use pgrx::prelude::*;
use ring::{digest, hmac, rand::SecureRandom};
use subtle::ConstantTimeEq;

pub const MECHANISM: &str = "SCRAM-SHA-256";

/// Alphanumeric, not base64: RFC 5802 would allow base64's `+` and `/`, but librdkafka
const SERVER_NONCE_CHARS: usize = 32;

const NONCE_ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

const MAX_SASL_MESSAGE: usize = 4096;

#[derive(Debug, Clone)]
pub enum SaslState {
    AwaitingHandshake,
    AwaitingFirst,
    AwaitingFinal(Box<Pending>),
    Authenticated { principal: String },
}

#[derive(Debug, Clone)]
pub struct Pending {
    principal: String,
    /// `client-first-message-bare`, needed verbatim for the auth message.
    client_first_bare: String,
    server_first: String,
    client_nonce: String,
    server_nonce: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
    real: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslError {
    UnsupportedMechanism(String),
    IllegalState(&'static str),
    Failed(&'static str),
}

impl SaslError {
    pub fn error_code(&self) -> kafgres_codec::ErrorCode {
        use kafgres_codec::ErrorCode as E;
        match self {
            SaslError::UnsupportedMechanism(_) => E::UnsupportedSaslMechanism,
            SaslError::IllegalState(_) => E::IllegalSaslState,
            SaslError::Failed(_) => E::SaslAuthenticationFailed,
        }
    }
}

impl std::fmt::Display for SaslError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslError::UnsupportedMechanism(m) => write!(f, "unsupported mechanism '{m}'"),
            SaslError::IllegalState(m) => write!(f, "illegal SASL state: {m}"),
            SaslError::Failed(m) => write!(f, "authentication failed: {m}"),
        }
    }
}

struct Verifier {
    iterations: u32,
    salt: Vec<u8>,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn parse_verifier(s: &str) -> Option<Verifier> {
    let rest = s.strip_prefix("SCRAM-SHA-256$")?;
    let (iter_salt, keys) = rest.split_once('$')?;
    let (iter_str, salt_b64) = iter_salt.split_once(':')?;
    let (stored_b64, server_b64) = keys.split_once(':')?;

    let iterations = iter_str.parse::<u32>().ok()?;
    if iterations == 0 {
        return None;
    }
    let salt = b64().decode(salt_b64).ok()?;
    let stored_key = b64().decode(stored_b64).ok()?;
    let server_key = b64().decode(server_b64).ok()?;
    if stored_key.len() != 32 || server_key.len() != 32 {
        return None;
    }
    Some(Verifier {
        iterations,
        salt,
        stored_key,
        server_key,
    })
}

const SCRAM_SALT_LEN: usize = 16;

/// A verifier that cannot match, for a missing role: the exchange must be indistinguishable
fn dummy_verifier(username: &str, cluster_secret: &[u8], iterations: u32) -> Verifier {
    let mut salt = hmac256(cluster_secret, username.as_bytes());
    salt.truncate(SCRAM_SALT_LEN);
    Verifier {
        iterations,
        salt,
        stored_key: vec![0u8; 32],
        server_key: vec![0u8; 32],
    }
}

struct Lookup {
    verifier: Option<Verifier>,
    cluster_secret: Vec<u8>,
    iterations: u32,
}

/// One row on either branch: a differing query count is its own timing oracle.
fn lookup(username: &str) -> Result<Lookup, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT (SELECT rolpassword FROM pg_authid
                      WHERE rolname = $1 AND rolcanlogin
                        AND (rolvaliduntil IS NULL OR rolvaliduntil > now())),
                    (SELECT system_identifier::text FROM pg_control_system()),
                    current_setting('scram_iterations', true)",
            None,
            &[username.into()],
        )?;
        let mut out = Lookup {
            verifier: None,
            cluster_secret: b"kafgres-fallback-mock-key".to_vec(),
            iterations: 4096,
        };
        for row in rows {
            out.verifier = row.get::<String>(1)?.as_deref().and_then(parse_verifier);
            if let Some(id) = row.get::<String>(2)? {
                out.cluster_secret = id.into_bytes();
            }
            if let Some(i) = row.get::<String>(3)?.and_then(|v| v.parse::<u32>().ok()) {
                if i > 0 {
                    out.iterations = i;
                }
            }
        }
        Ok(out)
    })
}

fn hmac256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, msg).as_ref().to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    digest::digest(&digest::SHA256, data).as_ref().to_vec()
}

pub fn handshake(state: &SaslState, mechanism: &str) -> Result<SaslState, SaslError> {
    if !matches!(state, SaslState::AwaitingHandshake) {
        return Err(SaslError::IllegalState("handshake already done"));
    }
    if mechanism != MECHANISM {
        return Err(SaslError::UnsupportedMechanism(mechanism.to_string()));
    }
    Ok(SaslState::AwaitingFirst)
}

pub fn step(state: &SaslState, token: &[u8]) -> Result<(Vec<u8>, SaslState), SaslError> {
    if token.len() > MAX_SASL_MESSAGE {
        return Err(SaslError::Failed("message too large"));
    }
    match state {
        SaslState::AwaitingHandshake => Err(SaslError::IllegalState("no handshake yet")),
        SaslState::AwaitingFirst => client_first(token),
        SaslState::AwaitingFinal(pending) => client_final(pending, token),
        SaslState::Authenticated { .. } => Err(SaslError::IllegalState("already authenticated")),
    }
}

fn client_first(token: &[u8]) -> Result<(Vec<u8>, SaslState), SaslError> {
    let msg = std::str::from_utf8(token).map_err(|_| SaslError::Failed("client-first not utf-8"))?;

    // gs2-header: `y,,` is a channel-binding downgrade signal (RFC 5802 says fail), and
    let bare = if let Some(rest) = msg.strip_prefix("n,,") {
        rest
    } else if let Some(rest) = msg.strip_prefix("y,,") {
        let _ = rest;
        return Err(SaslError::Failed("channel binding downgrade"));
    } else if msg.starts_with("p=") {
        return Err(SaslError::Failed("channel binding not supported"));
    } else {
        return Err(SaslError::Failed("unsupported gs2 header"));
    };

    let mut username = None;
    let mut client_nonce = None;
    for field in bare.split(',') {
        match field.split_once('=') {
            Some(("n", v)) => username = Some(saslname_decode(v)?),
            Some(("r", v)) => client_nonce = Some(v.to_string()),
            _ => {}
        }
    }
    let username = username.ok_or(SaslError::Failed("no username in client-first"))?;
    let client_nonce = client_nonce.ok_or(SaslError::Failed("no nonce in client-first"))?;
    if client_nonce.is_empty() || !client_nonce.bytes().all(printable_nonce) {
        return Err(SaslError::Failed("malformed client nonce"));
    }

    let found = lookup(&username).unwrap_or_else(|e| {
        pgrx::log!("kafgres: sasl role lookup failed: {e}");
        Lookup {
            verifier: None,
            cluster_secret: b"kafgres-fallback-mock-key".to_vec(),
            iterations: 4096,
        }
    });
    let real = found.verifier.is_some();
    let verifier = found.verifier.unwrap_or_else(|| {
        dummy_verifier(&username, &found.cluster_secret, found.iterations)
    });

    let server_nonce = server_nonce()?;

    let server_first = format!(
        "r={client_nonce}{server_nonce},s={},i={}",
        b64().encode(&verifier.salt),
        verifier.iterations
    );

    Ok((
        server_first.clone().into_bytes(),
        SaslState::AwaitingFinal(Box::new(Pending {
            principal: username,
            client_first_bare: bare.to_string(),
            server_first,
            client_nonce,
            server_nonce,
            stored_key: verifier.stored_key,
            server_key: verifier.server_key,
            real,
        })),
    ))
}

fn client_final(pending: &Pending, token: &[u8]) -> Result<(Vec<u8>, SaslState), SaslError> {
    let msg = std::str::from_utf8(token).map_err(|_| SaslError::Failed("client-final not utf-8"))?;

    let mut channel_binding = None;
    let mut nonce = None;
    let mut proof_b64 = None;
    for field in msg.split(',') {
        match field.split_once('=') {
            Some(("c", v)) => channel_binding = Some(v.to_string()),
            Some(("r", v)) => nonce = Some(v.to_string()),
            Some(("p", v)) => proof_b64 = Some(v.to_string()),
            _ => {}
        }
    }

    // `biws` is base64("n,,"): anything else is a gs2 header the client did not open with.
    match channel_binding.as_deref() {
        Some("biws") => {}
        _ => return Err(SaslError::Failed("channel binding mismatch")),
    }

    // Prefix/suffix match, not equality: librdkafka echoes `cnonce + <whole r=>` while the
    let echoed = nonce.as_deref().unwrap_or("");
    if !echoed.starts_with(&pending.client_nonce) || !echoed.ends_with(&pending.server_nonce) {
        return Err(SaslError::Failed("nonce not bound to the challenge"));
    }

    let proof_b64 = proof_b64.ok_or(SaslError::Failed("no proof in client-final"))?;
    let proof = b64()
        .decode(proof_b64.as_bytes())
        .map_err(|_| SaslError::Failed("proof not base64"))?;
    if proof.len() != 32 {
        return Err(SaslError::Failed("proof wrong length"));
    }

    let without_proof = msg
        .rsplit_once(",p=")
        .map(|(head, _)| head)
        .ok_or(SaslError::Failed("no proof delimiter"))?;
    let auth_message = format!(
        "{},{},{}",
        pending.client_first_bare, pending.server_first, without_proof
    );

    let client_signature = hmac256(&pending.stored_key, auth_message.as_bytes());
    let client_key: Vec<u8> = proof
        .iter()
        .zip(client_signature.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let computed = sha256(&client_key);

    // Constant-time, and both failure modes answer identically: an early return on `!real`
    let matches: bool = computed.ct_eq(&pending.stored_key).into();
    if !matches || !pending.real {
        return Err(SaslError::Failed("proof did not verify"));
    }

    let server_signature = hmac256(&pending.server_key, auth_message.as_bytes());
    let server_final = format!("v={}", b64().encode(server_signature));
    Ok((
        server_final.into_bytes(),
        SaslState::Authenticated {
            principal: pending.principal.clone(),
        },
    ))
}

/// Rejection sampling: `% NONCE_ALPHABET.len()` would bias the head of the alphabet.
fn server_nonce() -> Result<String, SaslError> {
    let mut out = String::with_capacity(SERVER_NONCE_CHARS);
    let rng = ring::rand::SystemRandom::new();
    let mut buf = [0u8; 64];
    while out.len() < SERVER_NONCE_CHARS {
        rng.fill(&mut buf)
            .map_err(|_| SaslError::Failed("no entropy for server nonce"))?;
        for b in buf {
            if out.len() == SERVER_NONCE_CHARS {
                break;
            }
            if (b as usize) < 256 - (256 % NONCE_ALPHABET.len()) {
                out.push(NONCE_ALPHABET[b as usize % NONCE_ALPHABET.len()] as char);
            }
        }
    }
    Ok(out)
}

fn saslname_decode(s: &str) -> Result<String, SaslError> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '=' {
            out.push(c);
            continue;
        }
        match (chars.next(), chars.next()) {
            (Some('2'), Some('C')) => out.push(','),
            (Some('3'), Some('D')) => out.push('='),
            _ => return Err(SaslError::Failed("bad escape in username")),
        }
    }
    if out.is_empty() {
        return Err(SaslError::Failed("empty username"));
    }
    Ok(out)
}

fn printable_nonce(b: u8) -> bool {
    (0x21..=0x7e).contains(&b) && b != b','
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_postgres_verifier_parses() {
        let salt = b64().encode([1u8; 16]);
        let stored = b64().encode([2u8; 32]);
        let server = b64().encode([3u8; 32]);
        let v = parse_verifier(&format!("SCRAM-SHA-256$4096:{salt}${stored}:{server}"))
            .expect("should parse");
        assert_eq!(v.iterations, 4096);
        assert_eq!(v.salt.len(), 16);
        assert_eq!(v.stored_key, vec![2u8; 32]);
        assert_eq!(v.server_key, vec![3u8; 32]);
    }

    #[test]
    fn an_md5_verifier_is_not_usable() {
        assert!(parse_verifier("md5abcdef0123456789abcdef01234567").is_none());
        assert!(parse_verifier("").is_none());
        assert!(parse_verifier("SCRAM-SHA-256$notanumber:c2FsdA==$a:b").is_none());
    }

    #[test]
    fn a_truncated_verifier_is_not_usable() {
        let short = b64().encode([2u8; 16]);
        let salt = b64().encode([1u8; 16]);
        let server = b64().encode([3u8; 32]);
        assert!(parse_verifier(&format!("SCRAM-SHA-256$4096:{salt}${short}:{server}")).is_none());
    }

    #[test]
    fn only_scram_sha_256_is_offered() {
        let s = SaslState::AwaitingHandshake;
        assert!(handshake(&s, MECHANISM).is_ok());
        assert!(matches!(
            handshake(&s, "PLAIN"),
            Err(SaslError::UnsupportedMechanism(_))
        ));
        assert!(matches!(
            handshake(&s, "SCRAM-SHA-512"),
            Err(SaslError::UnsupportedMechanism(_))
        ));
    }

    #[test]
    fn the_exchange_cannot_be_taken_out_of_order() {
        assert!(matches!(
            step(&SaslState::AwaitingHandshake, b"n,,n=u,r=abc"),
            Err(SaslError::IllegalState(_))
        ));
        assert!(matches!(
            handshake(&SaslState::AwaitingFirst, MECHANISM),
            Err(SaslError::IllegalState(_))
        ));
        assert!(matches!(
            step(
                &SaslState::Authenticated {
                    principal: "u".into()
                },
                b"anything"
            ),
            Err(SaslError::IllegalState(_))
        ));
    }

    #[test]
    fn channel_binding_downgrade_is_refused() {
        assert!(matches!(
            step(&SaslState::AwaitingFirst, b"y,,n=user,r=nonce"),
            Err(SaslError::Failed("channel binding downgrade"))
        ));
    }

    #[test]
    fn usernames_are_unescaped_per_rfc_5802() {
        assert_eq!(saslname_decode("plain").unwrap(), "plain");
        assert_eq!(saslname_decode("a=2Cb").unwrap(), "a,b");
        assert_eq!(saslname_decode("a=3Db").unwrap(), "a=b");
        assert!(saslname_decode("a=b").is_err());
        assert!(saslname_decode("").is_err());
    }

    #[test]
    fn oversized_tokens_are_refused_before_parsing() {
        let big = vec![b'a'; MAX_SASL_MESSAGE + 1];
        assert!(matches!(
            step(&SaslState::AwaitingFirst, &big),
            Err(SaslError::Failed("message too large"))
        ));
    }
}

pub fn scram_users(only: &[String]) -> Result<Vec<(String, i32)>, String> {
    // Filtered in Rust rather than with `rolname = ANY($1::text[])`: binding a `Vec<String>`
    let all = Spi::connect(|client| {
        let rows = client.select(
            "SELECT rolname::text, rolpassword FROM pg_authid
              WHERE rolcanlogin
                AND (rolvaliduntil IS NULL OR rolvaliduntil > now())
                AND rolpassword LIKE 'SCRAM-SHA-256$%'
              ORDER BY rolname",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for row in rows {
            let (Some(name), Some(verifier)) = (row.get::<String>(1)?, row.get::<String>(2)?)
            else {
                continue;
            };
            if let Some(v) = parse_verifier(&verifier) {
                out.push((name, v.iterations as i32));
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .map_err(|e| format!("cannot list SCRAM roles: {e}"))?;

    if only.is_empty() {
        return Ok(all);
    }
    Ok(all
        .into_iter()
        .filter(|(name, _)| only.iter().any(|w| w == name))
        .collect())
}

pub fn postgres_verifier(iterations: i32, salt: &[u8], salted_password: &[u8]) -> String {
    let client_key = hmac256(salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac256(salted_password, b"Server Key");
    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        iterations,
        b64().encode(salt),
        b64().encode(stored_key),
        b64().encode(server_key)
    )
}
