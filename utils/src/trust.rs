use std::{collections::BTreeMap, fs, io, path::PathBuf, sync::Mutex};

use crate::tls::data_dir;

const FILE: &str = "known_peers";

static LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trust {
    New,
    Known,
    Changed { previous: String },
}

fn path() -> PathBuf {
    data_dir().join(FILE)
}

fn usable_name(name: &str) -> bool {
    !name.is_empty() && !name.chars().any(char::is_control)
}

fn load() -> BTreeMap<String, String> {
    fs::read_to_string(path())
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.split_once(' '))
        .filter(|(_, name)| usable_name(name))
        .map(|(fp, name)| (name.to_string(), fp.to_string()))
        .collect()
}

fn save(peers: &BTreeMap<String, String>) -> io::Result<()> {
    let dir = data_dir();
    fs::create_dir_all(&dir)?;
    let body: String = peers
        .iter()
        .map(|(name, fp)| format!("{fp} {name}\n"))
        .collect();
    let tmp = dir.join(format!("{FILE}.tmp"));
    fs::write(&tmp, body)?;
    fs::rename(tmp, path())
}

pub fn check(name: &str, fingerprint: &str) -> Trust {
    let _guard = LOCK.lock().unwrap();
    let mut peers = load();
    match peers.get(name) {
        Some(known) if known == fingerprint => Trust::Known,
        Some(known) => Trust::Changed {
            previous: known.clone(),
        },
        None => {
            if usable_name(name) {
                peers.insert(name.to_string(), fingerprint.to_string());
                let _ = save(&peers);
            }
            Trust::New
        }
    }
}

pub fn remember(name: &str, fingerprint: &str) -> io::Result<()> {
    if !usable_name(name) {
        return Ok(());
    }
    let _guard = LOCK.lock().unwrap();
    let mut peers = load();
    peers.insert(name.to_string(), fingerprint.to_string());
    save(&peers)
}
