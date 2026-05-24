use anyhow::{bail, Context, Result};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const ALGORITHM: jsonwebtoken::Algorithm = jsonwebtoken::Algorithm::HS256;
const ISSUER: &str = "agora";
const DEFAULT_TOKEN_PATH: &str = ".kiro/session_token";
pub const DEFAULT_TTL_SECS: u64 = 15 * 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub iss: String,
    pub sub: String,
    pub obo: String,
    pub scp: Vec<String>,
    pub sid: String,
    pub iat: u64,
    pub exp: u64,
}

impl Claims {
    pub fn has_scope(&self, required: &str) -> bool {
        self.scp.iter().any(|granted| {
            required == granted
                || required
                    .strip_prefix(granted)
                    .is_some_and(|rest| rest.starts_with(':') || rest.starts_with('/'))
                || (granted.ends_with('/') && required.starts_with(granted))
        })
    }
}

pub fn load_signing_key(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let p = path.as_ref();
    let data = std::fs::read(p).with_context(|| {
        format!(
            "Signing key not found at {}. Run scripts/bootstrap.sh to mint one.",
            p.display()
        )
    })?;
    let trimmed = data
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect::<Vec<_>>();
    if trimmed.is_empty() {
        bail!("Signing key at {} is empty", p.display());
    }
    Ok(trimmed)
}

pub fn default_key_path() -> PathBuf {
    PathBuf::from(DEFAULT_TOKEN_PATH)
}

pub fn mint_actor_token(
    subject: &str,
    scopes: &[&str],
    session_id: &str,
    signing_key: &[u8],
    ttl_seconds: u64,
) -> Result<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let claims = Claims {
        iss: ISSUER.to_string(),
        sub: subject.to_string(),
        obo: subject.to_string(),
        scp: scopes.iter().map(|s| s.to_string()).collect(),
        sid: session_id.to_string(),
        iat: now,
        exp: now + ttl_seconds,
    };
    let mut header = Header::new(ALGORITHM);
    header.typ = Some("JWT".to_string());
    let token = encode(&header, &claims, &EncodingKey::from_secret(signing_key))?;
    Ok(token)
}

pub fn mint_default_token(session_id: &str, signing_key: &[u8]) -> Result<String> {
    mint_actor_token(
        "agora-daemon",
        &["workspace:read", "workspace:write", "shell:run:src/"],
        session_id,
        signing_key,
        DEFAULT_TTL_SECS,
    )
}

pub fn verify_actor_token(token: &str, signing_key: &[u8]) -> Result<Claims> {
    let mut validation = Validation::new(ALGORITHM);
    validation.set_issuer(&[ISSUER]);
    validation.set_required_spec_claims(&["exp", "iat", "sub", "scp", "sid"]);

    let data = decode::<Claims>(token, &DecodingKey::from_secret(signing_key), &validation)
        .context("Invalid actor token")?;
    Ok(data.claims)
}

#[cfg(test)]
mod tests {
    use super::{mint_actor_token, verify_actor_token, Claims};

    #[test]
    fn exact_and_hierarchical_scopes_match() {
        let claims = Claims {
            iss: "test".into(),
            sub: "subject".into(),
            obo: "subject".into(),
            scp: vec!["workspace:write".into(), "shell:run:src/".into()],
            sid: "sess_test".into(),
            iat: 0,
            exp: 1,
        };

        assert!(claims.has_scope("workspace:write"));
        assert!(claims.has_scope("workspace:write:files"));
        assert!(claims.has_scope("shell:run:src/main.rs"));
        assert!(!claims.has_scope("workspace:read"));
        assert!(!claims.has_scope("workspace:writer"));
    }

    #[test]
    fn minted_token_round_trips() {
        let key = b"test-signing-key";
        let token =
            mint_actor_token("console-user", &["workspace:read"], "sess_test", key, 60).unwrap();
        let claims = verify_actor_token(&token, key).unwrap();

        assert_eq!(claims.sub, "console-user");
        assert_eq!(claims.sid, "sess_test");
        assert!(claims.has_scope("workspace:read"));
    }
}
