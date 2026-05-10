//! TDS 8.0 Strict encryption integration tests (issue #62).
//!
//! Strict mode performs the TLS handshake **before** the TDS pre-login packet,
//! using ALPN with the `tds/8.0` protocol id. The cipher then wraps the entire
//! TDS conversation, including pre-login.
//!
//! These tests are gated on a Strict-capable endpoint (Azure SQL, or any local
//! SQL Server 2022 configured with a publicly-trusted certificate). Set:
//!
//!   STRICT_TEST_HOST     — server hostname that matches the cert CN/SAN
//!   STRICT_TEST_PORT     — defaults to 1433
//!   STRICT_TEST_DB       — defaults to master
//!   STRICT_TEST_USER     — defaults to sa
//!   STRICT_TEST_PASSWORD — required; tests are skipped if unset
//!   STRICT_TEST_CA       — optional path to a CA bundle (PEM); when unset the
//!                          system trust store is used (which is what an Azure
//!                          SQL endpoint requires).
//!
//! Without `STRICT_TEST_PASSWORD` the tests print a skip notice and exit
//! successfully so CI without a Strict endpoint stays green.

use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};

fn strict_config() -> Option<Config> {
    let password = std::env::var("STRICT_TEST_PASSWORD").ok()?;
    let host = std::env::var("STRICT_TEST_HOST").ok()?;
    let port: u16 = std::env::var("STRICT_TEST_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1433);
    let database = std::env::var("STRICT_TEST_DB").unwrap_or_else(|_| "master".into());
    let user = std::env::var("STRICT_TEST_USER").unwrap_or_else(|_| "sa".into());

    let mut cfg = Config::new();
    cfg.host(host)
        .port(port)
        .database(database)
        .authentication(AuthMethod::sql_server(user, password))
        .encryption(EncryptionLevel::Strict);

    if let Ok(ca) = std::env::var("STRICT_TEST_CA") {
        cfg.trust_cert_ca(ca);
    }

    Some(cfg)
}

fn skip_or(cfg: Option<Config>, name: &str) -> Option<Config> {
    if cfg.is_none() {
        eprintln!(
            "skipping {name}: set STRICT_TEST_HOST and STRICT_TEST_PASSWORD to enable Strict-mode integration tests"
        );
    }
    cfg
}

#[tokio::test]
async fn strict_select_one() {
    let Some(cfg) = skip_or(strict_config(), "strict_select_one") else {
        return;
    };

    let mut client = Client::connect(&cfg).await.expect("Strict connect failed");
    let rows = client
        .simple_query("SELECT 1 AS value")
        .await
        .expect("query failed")
        .into_first_result();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>("value"), Some(1));
}

#[tokio::test]
async fn strict_reports_encrypted_session() {
    // Asks the server whether the current session is encrypted. Under Strict
    // mode the value must be 'TRUE'.
    let Some(cfg) = skip_or(strict_config(), "strict_reports_encrypted_session") else {
        return;
    };

    let mut client = Client::connect(&cfg).await.expect("Strict connect failed");
    let rows = client
        .simple_query(
            "SELECT CAST(encrypt_option AS varchar(10)) AS encrypted \
             FROM sys.dm_exec_connections \
             WHERE session_id = @@SPID",
        )
        .await
        .expect("query failed")
        .into_first_result();

    let encrypted: String = rows[0]
        .get("encrypted")
        .expect("encrypt_option column missing");
    assert_eq!(
        encrypted.to_uppercase(),
        "TRUE",
        "Strict mode must yield an encrypted session, got {encrypted:?}"
    );
}

#[tokio::test]
async fn strict_ignores_trust_cert_flag() {
    // Even when the user calls `trust_cert()`, mssql-tds enforces real cert
    // validation under Strict. Against a properly-configured Strict endpoint
    // this should still succeed (because the real cert is valid).
    let Some(mut cfg) = skip_or(strict_config(), "strict_ignores_trust_cert_flag") else {
        return;
    };
    cfg.trust_cert();

    let mut client = Client::connect(&cfg)
        .await
        .expect("Strict connect with trust_cert() should still succeed against a valid cert");
    let rows = client
        .simple_query("SELECT 1 AS value")
        .await
        .expect("query failed")
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>("value"), Some(1));
}
