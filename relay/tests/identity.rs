use tokio::io::duplex;
use utils::{tls, trust};

fn isolated_identity() -> std::path::PathBuf {
    static ONCE: std::sync::Once = std::sync::Once::new();
    let dir = std::env::temp_dir().join(format!("cli-net-comm-id-tests-{}", std::process::id()));
    ONCE.call_once(|| unsafe {
        std::env::set_var("CLI_NET_COMM_HOME", &dir);
    });
    dir
}

#[tokio::test]
async fn direct_connection_shows_same_safety_code_and_peer_key() {
    isolated_identity();
    let (a, b) = duplex(64 * 1024);
    let server = tokio::spawn(async move { tls::accept(b).await.unwrap() });
    let (_client_stream, client_info) = tls::connect(a).await.unwrap();
    let (_server_stream, server_info) = server.await.unwrap();

    assert_eq!(client_info.safety_code, server_info.safety_code);
    assert_eq!(client_info.fingerprint, tls::identity().fingerprint);
    assert_eq!(server_info.fingerprint, tls::identity().fingerprint);
    println!("safety code {}", client_info.safety_code);
}

#[tokio::test]
async fn man_in_the_middle_gets_different_safety_codes() {
    isolated_identity();
    let (alice_end, mallory_from_alice) = duplex(64 * 1024);
    let (mallory_to_bob, bob_end) = duplex(64 * 1024);

    let bob = tokio::spawn(async move { tls::accept(bob_end).await.unwrap() });
    let mallory = tokio::spawn(async move {
        let a = tls::accept(mallory_from_alice).await.unwrap();
        let b = tls::connect(mallory_to_bob).await.unwrap();
        (a, b)
    });
    let (_alice_stream, alice) = tls::connect(alice_end).await.unwrap();
    let (_bob_stream, bob) = bob.await.unwrap();
    let _mallory_streams = mallory.await.unwrap();

    println!(
        "Alice sees {}, Bob sees {}",
        alice.safety_code, bob.safety_code
    );
    assert_ne!(alice.safety_code, bob.safety_code);
}

#[test]
fn known_peers_detect_changed_keys() {
    let dir = isolated_identity();
    let _ = std::fs::remove_file(dir.join("known_peers"));
    assert_eq!(trust::check("Bob", "AAAA-1111"), trust::Trust::New);
    assert_eq!(trust::check("Bob", "AAAA-1111"), trust::Trust::Known);
    assert_eq!(
        trust::check("Bob", "BBBB-2222"),
        trust::Trust::Changed {
            previous: "AAAA-1111".into()
        }
    );
    trust::remember("Bob", "BBBB-2222").unwrap();
    assert_eq!(trust::check("Bob", "BBBB-2222"), trust::Trust::Known);
    assert_eq!(trust::check("Pepa Novák", "CCCC"), trust::Trust::New);
    assert_eq!(trust::check("Pepa Novák", "CCCC"), trust::Trust::Known);
    assert_eq!(trust::check("bad\u{1b}name", "DDDD"), trust::Trust::New);
    assert_eq!(
        trust::check("bad\u{1b}name", "DDDD"),
        trust::Trust::New,
        "control chars are never stored"
    );
}

#[test]
fn identity_persists_on_disk_with_private_permissions() {
    let dir = isolated_identity();
    let fp = tls::identity().fingerprint.clone();
    let key = dir.join("identity.key.der");
    assert!(key.exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&key).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    println!("identity {fp} stored in {}", dir.display());
}
