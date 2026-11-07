//! Users, SCRAM-SHA-256 credentials and table privileges.
//!
//! ```text
//!  0x01 "user/" ‖ name → {superuser, login, scram: {salt, iterations, stored_key, server_key}, grants}
//! ```
//!
//! Only the SCRAM verifiers are stored (RFC 5802 / 7677, as Postgres does):
//! the server can check a client's proof and prove itself back, but cannot
//! recover the password. While no user exists the server trusts every
//! connection; the first `CREATE USER` turns authentication on for all of them.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::keycodec::{self, META};
use crate::kv::KvTxn;
use crate::{Error, Result};

pub const PRIVILEGES: [&str; 4] = ["SELECT", "INSERT", "UPDATE", "DELETE"];
pub const ITERATIONS: u32 = 4096;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Scram {
    pub salt: String,
    pub iterations: u32,
    pub stored_key: String,
    pub server_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct User {
    pub name: String,
    pub superuser: bool,
    pub login: bool,
    pub scram: Option<Scram>,
    /// Table → privileges.
    pub grants: BTreeMap<String, BTreeSet<String>>,
}

impl User {
    pub fn may(&self, table: &str, privilege: &str) -> bool {
        self.superuser
            || self
                .grants
                .get(table)
                .is_some_and(|p| p.contains(privilege))
    }
}

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

pub fn b64_public(b: &[u8]) -> String {
    b64(b)
}

pub fn unb64(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC takes any key length");
    m.update(msg);
    m.finalize().into_bytes().to_vec()
}

/// The verifiers for `password` (salt: 16 random bytes).
pub fn scram_secret(password: &str, salt: &[u8], iterations: u32) -> Scram {
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);
    let client_key = hmac(&salted, b"Client Key");
    let stored_key = Sha256::digest(&client_key).to_vec();
    let server_key = hmac(&salted, b"Server Key");
    Scram {
        salt: b64(salt),
        iterations,
        stored_key: b64(&stored_key),
        server_key: b64(&server_key),
    }
}

/// Check a client's proof over `auth_message`; on success, the server's
/// signature to send back (which proves the server knows the verifier too).
pub fn scram_verify(s: &Scram, auth_message: &str, proof: &[u8]) -> Option<Vec<u8>> {
    let stored = unb64(&s.stored_key)?;
    let server = unb64(&s.server_key)?;
    let signature = hmac(&stored, auth_message.as_bytes());
    if proof.len() != signature.len() {
        return None;
    }
    let client_key: Vec<u8> = proof.iter().zip(&signature).map(|(a, b)| a ^ b).collect();
    // Constant-time comparison of the derived key with the stored one.
    let derived = Sha256::digest(&client_key);
    let diff = derived
        .iter()
        .zip(&stored)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b));
    (diff == 0 && derived.len() == stored.len()).then(|| hmac(&server, auth_message.as_bytes()))
}

fn user_key(name: &str) -> Vec<u8> {
    let mut k = vec![META];
    k.extend_from_slice(b"user/");
    k.extend_from_slice(name.as_bytes());
    k
}

pub async fn get_user(txn: &mut dyn KvTxn, name: &str) -> Result<Option<User>> {
    match txn.get(&user_key(name)).await? {
        Some(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(|e| Error::Internal(format!("corrupt user record: {e}"))),
        None => Ok(None),
    }
}

pub async fn put_user(txn: &mut dyn KvTxn, u: &User) -> Result<()> {
    txn.put(
        &user_key(&u.name),
        &serde_json::to_vec(u).map_err(|e| Error::Internal(e.to_string()))?,
    )
    .await
}

pub async fn drop_user(txn: &mut dyn KvTxn, name: &str) -> Result<()> {
    txn.delete(&user_key(name)).await
}

/// Is there any user (and so, must every connection authenticate)?
pub async fn any_user(txn: &mut dyn KvTxn) -> Result<bool> {
    let start = user_key("");
    let end = keycodec::prefix_end(&start);
    Ok(!txn.scan(&start, &end, 1).await?.is_empty())
}

/// Must connections authenticate? (Is there any user?)
pub async fn required(store: &std::sync::Arc<dyn crate::kv::KvStore>) -> Result<bool> {
    let mut t = store
        .begin(stowage_kv::txn::Isolation::Serializable)
        .await?;
    let r = any_user(t.as_mut()).await;
    let _ = t.rollback().await;
    r
}

pub async fn lookup(
    store: &std::sync::Arc<dyn crate::kv::KvStore>,
    name: &str,
) -> Result<Option<User>> {
    let mut t = store
        .begin(stowage_kv::txn::Isolation::Serializable)
        .await?;
    let r = get_user(t.as_mut(), name).await;
    let _ = t.rollback().await;
    r
}

/// 16 bytes from the operating system's CSPRNG.
pub fn random_salt() -> [u8; 16] {
    let mut b = [0u8; 16];
    getrandom::getrandom(&mut b).expect("the OS random source");
    b
}

fn literal(e: &sqlparser::ast::Expr) -> Result<String> {
    match e {
        sqlparser::ast::Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(s.clone()),
            other => Err(Error::InvalidInput(format!(
                "expected a string, got {other}"
            ))),
        },
        other => Err(Error::InvalidInput(format!(
            "expected a string, got {other}"
        ))),
    }
}

/// `CREATE USER` / `CREATE ROLE`.
pub async fn create_role(
    txn: &mut dyn KvTxn,
    cr: &sqlparser::ast::CreateRole,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for n in &cr.names {
        let name = crate::binder::object_name(n)?;
        if get_user(txn, &name).await?.is_some() {
            if cr.if_not_exists {
                continue;
            }
            return Err(Error::DuplicateObject(format!("role \"{name}\"")));
        }
        let scram = match &cr.password {
            Some(sqlparser::ast::Password::Password(e)) => {
                Some(scram_secret(&literal(e)?, &random_salt(), ITERATIONS))
            }
            _ => None,
        };
        put_user(
            txn,
            &User {
                name: name.clone(),
                superuser: cr.superuser.unwrap_or(false),
                login: cr.login.unwrap_or(true),
                scram,
                grants: BTreeMap::new(),
            },
        )
        .await?;
        names.push(name);
    }
    Ok(names)
}

/// `ALTER ROLE name WITH PASSWORD … / SUPERUSER / LOGIN …`.
pub async fn alter_role(
    txn: &mut dyn KvTxn,
    name: &sqlparser::ast::Ident,
    op: &sqlparser::ast::AlterRoleOperation,
) -> Result<()> {
    use sqlparser::ast::{AlterRoleOperation, Password, RoleOption};
    let n = crate::binder::ident(name);
    let mut u = get_user(txn, &n)
        .await?
        .ok_or_else(|| Error::UndefinedObject(format!("role \"{n}\"")))?;
    match op {
        AlterRoleOperation::WithOptions { options } => {
            for o in options {
                match o {
                    RoleOption::Password(Password::Password(e)) => {
                        u.scram = Some(scram_secret(&literal(e)?, &random_salt(), ITERATIONS))
                    }
                    RoleOption::Password(Password::NullPassword) => u.scram = None,
                    RoleOption::SuperUser(b) => u.superuser = *b,
                    RoleOption::Login(b) => u.login = *b,
                    other => return Err(Error::Unsupported(format!("role option {other}"))),
                }
            }
        }
        other => return Err(Error::Unsupported(format!("ALTER ROLE {other}"))),
    }
    put_user(txn, &u).await
}

fn privileges(p: &sqlparser::ast::Privileges) -> Result<Vec<&'static str>> {
    use sqlparser::ast::{Action, Privileges};
    match p {
        Privileges::All { .. } => Ok(PRIVILEGES.to_vec()),
        Privileges::Actions(a) => a
            .iter()
            .map(|x| match x {
                Action::Select { .. } => Ok("SELECT"),
                Action::Insert { .. } => Ok("INSERT"),
                Action::Update { .. } => Ok("UPDATE"),
                Action::Delete => Ok("DELETE"),
                other => Err(Error::Unsupported(format!("privilege {other}"))),
            })
            .collect(),
    }
}

/// `GRANT …` (`grant = true`) or `REVOKE …` on tables to users.
pub async fn grant(
    txn: &mut dyn KvTxn,
    privs: &sqlparser::ast::Privileges,
    objects: Option<&sqlparser::ast::GrantObjects>,
    grantees: &[sqlparser::ast::Grantee],
    grant: bool,
) -> Result<String> {
    use sqlparser::ast::{GrantObjects, GranteeName};
    let ps = privileges(privs)?;
    let tables: Vec<String> = match objects {
        Some(GrantObjects::Tables(t)) => t
            .iter()
            .map(crate::binder::object_name)
            .collect::<Result<_>>()?,
        Some(GrantObjects::AllTablesInSchema { .. }) => crate::catalog::list_tables(txn)
            .await?
            .into_iter()
            .map(|t| t.name)
            .collect(),
        other => return Err(Error::Unsupported(format!("GRANT on {other:?}"))),
    };
    for t in &tables {
        crate::catalog::table(txn, t).await?;
    }
    let mut who = Vec::new();
    for g in grantees {
        let Some(GranteeName::ObjectName(n)) = &g.name else {
            return Err(Error::Unsupported("grantee".into()));
        };
        let name = crate::binder::object_name(n)?;
        let mut u = get_user(txn, &name)
            .await?
            .ok_or_else(|| Error::UndefinedObject(format!("role \"{name}\"")))?;
        for t in &tables {
            let set = u.grants.entry(t.clone()).or_default();
            for p in &ps {
                if grant {
                    set.insert((*p).to_owned());
                } else {
                    set.remove(*p);
                }
            }
        }
        u.grants.retain(|_, s| !s.is_empty());
        put_user(txn, &u).await?;
        who.push(name);
    }
    Ok(format!(
        "{} {} on {} to {}",
        if grant { "grant" } else { "revoke" },
        ps.join(","),
        tables.join(","),
        who.join(",")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677's test vector for SCRAM-SHA-256 (user "user", password "pencil").
    #[test]
    fn rfc7677_vector() {
        let salt = unb64("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let s = scram_secret("pencil", &salt, 4096);
        let client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final_no_proof = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let auth = format!("{client_first_bare},{server_first},{client_final_no_proof}");
        let proof = unb64("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=").unwrap();
        let sig = scram_verify(&s, &auth, &proof).expect("the RFC's proof must verify");
        assert_eq!(b64(&sig), "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
        let mut bad = proof.clone();
        bad[0] ^= 1;
        assert!(scram_verify(&s, &auth, &bad).is_none());
    }
}
