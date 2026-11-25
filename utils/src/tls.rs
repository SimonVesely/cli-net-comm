use std::{
    fs, io,
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::Duration,
};

use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, ring as ring_provider},
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
    server::danger::{ClientCertVerified, ClientCertVerifier},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

const SERVER_NAME: &str = "cli-net-comm";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SAFETY_LABEL: &[u8] = b"EXPORTER-cli-net-comm-safety-code";
const CERT_FILE: &str = "identity.cert.der";
const KEY_FILE: &str = "identity.key.der";

pub struct Identity {
    server_config: Arc<ServerConfig>,
    client_config: Arc<ClientConfig>,
    pub fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerInfo {
    pub fingerprint: String,
    pub safety_code: String,
}

static IDENTITY: OnceLock<Identity> = OnceLock::new();

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring_provider::default_provider())
}

pub fn data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("CLI_NET_COMM_HOME") {
        return PathBuf::from(dir);
    }
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".cli-net-comm")
}

fn write_private(path: &PathBuf, data: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(data)
    }
    #[cfg(not(unix))]
    fs::write(path, data)
}

fn load_or_create() -> io::Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let dir = data_dir();
    let (cert_path, key_path) = (dir.join(CERT_FILE), dir.join(KEY_FILE));
    if let (Ok(cert), Ok(key)) = (fs::read(&cert_path), fs::read(&key_path)) {
        return Ok((
            CertificateDer::from(cert),
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)),
        ));
    }

    fs::create_dir_all(&dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    }
    let generated = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])
        .map_err(io::Error::other)?;
    let cert = generated.cert.der().to_vec();
    let key = generated.signing_key.serialize_der();
    write_private(&key_path, &key)?;
    write_private(&cert_path, &cert)?;
    Ok((
        CertificateDer::from(cert),
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)),
    ))
}

pub fn identity() -> &'static Identity {
    IDENTITY.get_or_init(|| {
        let (cert, key) = load_or_create().expect("could not load or create identity key");
        let fingerprint = fingerprint(&cert);
        let provider = provider();

        let server_config = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3 supported by ring provider")
            .with_client_cert_verifier(Arc::new(AcceptAnyCert(provider.clone())))
            .with_single_cert(vec![cert.clone()], key.clone_key())
            .expect("valid identity certificate");

        let client_config = ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3 supported by ring provider")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
            .with_client_auth_cert(vec![cert], key)
            .expect("valid identity certificate");

        Identity {
            server_config: Arc::new(server_config),
            client_config: Arc::new(client_config),
            fingerprint,
        }
    })
}

pub fn fingerprint(cert: &CertificateDer<'_>) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, cert.as_ref());
    digest.as_ref()[..16]
        .chunks(2)
        .map(|pair| format!("{:02X}{:02X}", pair[0], pair[1]))
        .collect::<Vec<_>>()
        .join("-")
}

fn safety_code(bytes: [u8; 8]) -> String {
    let digits = format!("{:012}", u64::from_be_bytes(bytes) % 1_000_000_000_000);
    format!("{} {} {}", &digits[..4], &digits[4..8], &digits[8..])
}

#[derive(Debug)]
struct AcceptAnyCert(Arc<CryptoProvider>);

impl AcceptAnyCert {
    fn tls12(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn tls13(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes()
    }
}

impl ClientCertVerifier for AcceptAnyCert {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes()
    }
}

fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out")
}

fn peer_info<D>(conn: &rustls::ConnectionCommon<D>) -> io::Result<PeerInfo> {
    let cert = conn
        .peer_certificates()
        .and_then(|certs| certs.first())
        .ok_or_else(|| io::Error::other("peer sent no certificate"))?;
    let code = conn
        .export_keying_material([0u8; 8], SAFETY_LABEL, None)
        .map_err(io::Error::other)?;
    Ok(PeerInfo {
        fingerprint: fingerprint(cert),
        safety_code: safety_code(code),
    })
}

pub async fn connect<S>(stream: S) -> io::Result<(client::TlsStream<S>, PeerInfo)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let connector = TlsConnector::from(identity().client_config.clone());
    let name = ServerName::try_from(SERVER_NAME).expect("valid server name");
    let tls = timeout(HANDSHAKE_TIMEOUT, connector.connect(name, stream))
        .await
        .map_err(|_| timed_out())??;
    let info = peer_info(tls.get_ref().1)?;
    Ok((tls, info))
}

pub async fn accept<S>(stream: S) -> io::Result<(server::TlsStream<S>, PeerInfo)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let acceptor = TlsAcceptor::from(identity().server_config.clone());
    let tls = timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream))
        .await
        .map_err(|_| timed_out())??;
    let info = peer_info(tls.get_ref().1)?;
    Ok((tls, info))
}
