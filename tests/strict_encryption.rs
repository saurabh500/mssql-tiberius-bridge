//! TDS 8.0 Strict encryption integration tests (issue #62).
//!
//! Strict mode performs the TLS handshake **before** the TDS pre-login packet,
//! using ALPN with the `tds/8.0` protocol id. The cipher then wraps the entire
//! TDS conversation, including pre-login.
//!
//! These tests reuse the same `TEST_DB_*` env vars as `tests/integration.rs`
//! because a single SQL Server instance can accept both Strict and non-Strict
//! connections from the same listener (when `network.forceencryption=0` and a
//! TLS cert is configured). See issue #74 for the matching test-infra setup.
//!
//! Gates:
//!   `BRIDGE_STRICT_READY=1` — opt-in flag asserting the test server has a
//!                             TLS cert provisioned and is reachable at a
//!                             hostname that matches a SAN on that cert.
//!                             Without this flag the tests skip, because the
//!                             default CI SQL Server container is *not* yet
//!                             Strict-capable (tracked by #74). Same pattern
//!                             as `BRIDGE_AAD_TOKEN`, `BRIDGE_SSRP_HOST`,
//!                             `BRIDGE_CUSTOM_CA_PATH` in sibling tests.
//!   `TEST_DB_PASSWORD`     — required (also required by every other live
//!                             integration test).
//! Optional:
//!   `TEST_DB_HOST` (default: localhost), `TEST_DB_PORT` (1433),
//!   `TEST_DB_USER` (sa), `TEST_DB_NAME` (master),
//!   `TEST_DB_CA`   — path to a CA bundle (PEM). When unset the system trust
//!                    store is used. Strict requires real cert validation, so
//!                    `TEST_DB_HOST` must match a SAN on the server cert.

use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};

fn strict_config() -> Option<Config> {
    // Two gates, both must be satisfied:
    //  1. BRIDGE_STRICT_READY — server is provisioned for Strict (#74).
    //  2. TEST_DB_PASSWORD    — same secret the rest of the live suite uses.
    if std::env::var("BRIDGE_STRICT_READY").ok().as_deref() != Some("1") {
        return None;
    }
    let password = std::env::var("TEST_DB_PASSWORD").ok()?;

    let mut cfg = Config::new();
    cfg.host(std::env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            std::env::var("TEST_DB_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1433),
        )
        .database(std::env::var("TEST_DB_NAME").unwrap_or_else(|_| "master".into()))
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or_else(|_| "sa".into()),
            password,
        ))
        .encryption(EncryptionLevel::Strict);

    if let Ok(ca) = std::env::var("TEST_DB_CA") {
        cfg.trust_cert_ca(ca);
    }

    Some(cfg)
}

fn skip_or(cfg: Option<Config>, name: &str) -> Option<Config> {
    if cfg.is_none() {
        eprintln!(
            "skipping {name}: requires BRIDGE_STRICT_READY=1 and TEST_DB_PASSWORD against a Strict-capable SQL Server (see #74)"
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
