use std::{
    io,
    sync::{Arc, OnceLock},
    time::Duration,
};

use rustls::{
    ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, ring as ring_provider},
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
};
use tokio::{net::TcpStream, time::timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

const SERVER_NAME: &str = "cli-net-comm";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Identity {
    server_config: Arc<ServerConfig>,
    pub fingerprint: String,
}

static IDENTITY: OnceLock<Identity> = OnceLock::new();

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring_provider::default_provider())
}

pub fn identity() -> &'static Identity {
    IDENTITY.get_or_init(|| {
        let cert = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])
            .expect("self-signed certificate generation");
        let cert_der = CertificateDer::from(cert.cert.der().to_vec());
        let key_der =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
        let fingerprint = fingerprint(&cert_der);

        let server_config = ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3 supported by ring provider")
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .expect("valid self-signed cert");

        Identity {
            server_config: Arc::new(server_config),
            fingerprint,
        }
    })
}

pub fn fingerprint(cert: &CertificateDer<'_>) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, cert.as_ref());
    digest.as_ref()[..8]
        .chunks(2)
        .map(|pair| format!("{:02X}{:02X}", pair[0], pair[1]))
        .collect::<Vec<_>>()
        .join("-")
}

#[derive(Debug)]
struct AcceptAnyCert(Arc<CryptoProvider>);

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
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
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

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn client_config() -> Arc<ClientConfig> {
    let provider = provider();
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3 supported by ring provider")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
        .with_no_client_auth();
    Arc::new(config)
}

fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out")
}

pub async fn connect(stream: TcpStream) -> io::Result<(client::TlsStream<TcpStream>, String)> {
    let connector = TlsConnector::from(client_config());
    let name = ServerName::try_from(SERVER_NAME).expect("valid server name");
    let tls = timeout(HANDSHAKE_TIMEOUT, connector.connect(name, stream))
        .await
        .map_err(|_| timed_out())??;

    let peer_fp = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .map(fingerprint)
        .ok_or_else(|| io::Error::other("peer sent no certificate"))?;
    Ok((tls, peer_fp))
}

pub async fn accept(stream: TcpStream) -> io::Result<(server::TlsStream<TcpStream>, String)> {
    let id = identity();
    let acceptor = TlsAcceptor::from(id.server_config.clone());
    let tls = timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream))
        .await
        .map_err(|_| timed_out())??;
    Ok((tls, id.fingerprint.clone()))
}
