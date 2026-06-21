use std::collections::HashMap;
use std::path::{Path, PathBuf};

use peeroxide::KeyPair;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

const IDENTITY_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid topic hex")]
    InvalidTopic,
    #[error("invalid stored keypair")]
    InvalidKeyPair,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedKeyPair {
    #[serde(rename = "publicKey")]
    public_key: String,
    #[serde(rename = "secretKey")]
    secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdentityFile {
    version: u32,
    topics: HashMap<String, SerializedKeyPair>,
}

/// Persists per-topic Noise keypairs — wire-compatible with the JS `PeerIdentityStore`.
pub struct PeerIdentityStore {
    root: PathBuf,
    file_path: PathBuf,
    cache: Mutex<Option<IdentityFile>>,
}

impl PeerIdentityStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        Self {
            file_path: root.join("topic-keys.json"),
            root,
            cache: Mutex::new(None),
        }
    }

    pub async fn get_or_create(&self, topic_hex: &str) -> Result<KeyPair, IdentityError> {
        let key = normalize_topic(topic_hex)?;
        let mut cache = self.cache.lock().await;
        let mut file = self.load_locked(&mut cache).await?;

        if let Some(existing) = file.topics.get(&key) {
            return deserialize_keypair(existing);
        }

        let fresh = KeyPair::generate();
        file.topics.insert(key, serialize_keypair(&fresh));
        self.save_locked(&mut cache, &file).await?;
        Ok(fresh)
    }

    pub async fn delete(&self, topic_hex: &str) -> Result<(), IdentityError> {
        let key = normalize_topic(topic_hex)?;
        let mut cache = self.cache.lock().await;
        let mut file = self.load_locked(&mut cache).await?;
        if file.topics.remove(&key).is_some() {
            self.save_locked(&mut cache, &file).await?;
        }
        Ok(())
    }

    pub async fn clear(&self) -> Result<(), IdentityError> {
        let mut cache = self.cache.lock().await;
        let file = IdentityFile {
            version: IDENTITY_VERSION,
            topics: HashMap::new(),
        };
        self.save_locked(&mut cache, &file).await
    }

    async fn load_locked(
        &self,
        cache: &mut Option<IdentityFile>,
    ) -> Result<IdentityFile, IdentityError> {
        if let Some(file) = cache.clone() {
            return Ok(file);
        }

        tokio::fs::create_dir_all(&self.root).await?;

        let file = match tokio::fs::read_to_string(&self.file_path).await {
            Ok(raw) => match serde_json::from_str::<IdentityFile>(&raw) {
                Ok(parsed) if parsed.version == IDENTITY_VERSION => IdentityFile {
                    version: IDENTITY_VERSION,
                    topics: sanitize_topics(parsed.topics),
                },
                _ => IdentityFile {
                    version: IDENTITY_VERSION,
                    topics: HashMap::new(),
                },
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => IdentityFile {
                version: IDENTITY_VERSION,
                topics: HashMap::new(),
            },
            Err(err) => return Err(err.into()),
        };

        *cache = Some(file.clone());
        Ok(file)
    }

    async fn save_locked(
        &self,
        cache: &mut Option<IdentityFile>,
        file: &IdentityFile,
    ) -> Result<(), IdentityError> {
        tokio::fs::create_dir_all(&self.root).await?;
        let tmp = self.file_path.with_extension("json.tmp");
        let body = serde_json::to_vec(file)?;
        tokio::fs::write(&tmp, body).await?;
        tokio::fs::rename(&tmp, &self.file_path).await?;
        *cache = Some(file.clone());
        Ok(())
    }
}

fn normalize_topic(topic_hex: &str) -> Result<String, IdentityError> {
    let bytes = hex::decode(topic_hex).map_err(|_| IdentityError::InvalidTopic)?;
    if bytes.len() != 32 {
        return Err(IdentityError::InvalidTopic);
    }
    Ok(topic_hex.to_lowercase())
}

fn is_valid_hex_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() % 2 == 0
        && value.len() <= 128
        && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn sanitize_topics(raw: HashMap<String, SerializedKeyPair>) -> HashMap<String, SerializedKeyPair> {
    raw.into_iter()
        .filter(|(topic, entry)| {
            is_valid_hex_key(topic)
                && is_valid_hex_key(&entry.public_key)
                && is_valid_hex_key(&entry.secret_key)
        })
        .map(|(topic, entry)| (topic.to_lowercase(), entry))
        .collect()
}

fn serialize_keypair(key_pair: &KeyPair) -> SerializedKeyPair {
    SerializedKeyPair {
        public_key: hex::encode(key_pair.public_key),
        secret_key: hex::encode(key_pair.secret_key),
    }
}

fn deserialize_keypair(serialized: &SerializedKeyPair) -> Result<KeyPair, IdentityError> {
    let public = hex::decode(&serialized.public_key).map_err(|_| IdentityError::InvalidKeyPair)?;
    let secret = hex::decode(&serialized.secret_key).map_err(|_| IdentityError::InvalidKeyPair)?;
    if public.len() != 32 || secret.len() != 64 {
        return Err(IdentityError::InvalidKeyPair);
    }
    let mut public_key = [0u8; 32];
    let mut secret_key = [0u8; 64];
    public_key.copy_from_slice(&public);
    secret_key.copy_from_slice(&secret);
    Ok(KeyPair {
        public_key,
        secret_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrips_topic_keypair() {
        let dir = std::env::temp_dir().join(format!(
            "altersend-identity-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = PeerIdentityStore::new(&dir);
        let topic = hex::encode([7u8; 32]);

        let first = store.get_or_create(&topic).await.unwrap();
        let second = store.get_or_create(&topic).await.unwrap();
        assert_eq!(first.public_key, second.public_key);
        assert_eq!(first.secret_key, second.secret_key);
    }
}
