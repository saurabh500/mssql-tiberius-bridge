//! Integration tests for `Config::trust_cert_ca` (issue #18).
//!
//! Mirrors `tiberius/tests/custom-cert.rs`. The positive test
//! (`connect_with_trusted_ca`) requires a PEM/DER file matching the SQL
//! Server's TLS certificate and is run only when `BRIDGE_CUSTOM_CA_PATH`
//! is set.

use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};

fn base_config() -> Config {
    let password = std::env::var("TEST_DB_PASSWORD").unwrap_or_else(|_| {
        eprintln!("TEST_DB_PASSWORD not set, skipping");
        std::process::exit(0);
    });
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
        ));
    cfg
}

/// Negative case: encryption required, no CA pinned, no trust_cert ⇒ connect fails.
/// Equivalent to tiberius `connect_to_custom_cert_instance_without_ca`.
#[tokio::test]
async fn connect_without_ca_fails() {
    let mut cfg = base_config();
    cfg.encryption(EncryptionLevel::On);
    let result = Client::connect(&cfg).await;
    assert!(
        result.is_err(),
        "expected TLS validation to fail when no CA is trusted"
    );
}

/// Positive case: pin the CA cert and connect successfully.
/// Set `BRIDGE_CUSTOM_CA_PATH` to a PEM/DER file that matches the
/// server's TLS certificate. Mirrors tiberius `connect_to_custom_cert_instance_*`.
#[tokio::test]
async fn connect_with_trusted_ca() {
    let Ok(ca_path) = std::env::var("BRIDGE_CUSTOM_CA_PATH") else {
        eprintln!("BRIDGE_CUSTOM_CA_PATH not set, skipping");
        return;
    };
    let mut cfg = base_config();
    cfg.encryption(EncryptionLevel::On).trust_cert_ca(&ca_path);

    let mut client = Client::connect(&cfg)
        .await
        .expect("connect with pinned CA failed");

    let row = client
        .query("SELECT @P1", &[&-4i32])
        .await
        .expect("query failed")
        .into_first_result();
    assert_eq!(row[0].get::<i32, _>(0usize), Some(-4));
}
