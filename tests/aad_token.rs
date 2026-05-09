//! AAD/Entra ID federated auth integration test (issue #17).
//!
//! Gated on `BRIDGE_AAD_TOKEN` (a JWT for `https://database.windows.net/.default`)
//! plus the standard `TEST_DB_*` env vars pointing at an AAD-enabled SQL
//! Server (Azure SQL DB or Managed Instance).

use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};

#[tokio::test]
async fn aad_token_login() {
    let Ok(token) = std::env::var("BRIDGE_AAD_TOKEN") else {
        eprintln!("BRIDGE_AAD_TOKEN not set, skipping");
        return;
    };
    let host = std::env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".into());
    let port: u16 = std::env::var("TEST_DB_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1433);
    let database = std::env::var("TEST_DB_NAME").unwrap_or_else(|_| "master".into());

    let mut cfg = Config::new();
    cfg.host(host)
        .port(port)
        .database(database)
        .encryption(EncryptionLevel::Required)
        .authentication(AuthMethod::aad_token(token));

    let mut client = Client::connect(&cfg).await.expect("AAD login failed");
    let row = client
        .query("SELECT SUSER_SNAME()", &[])
        .await
        .unwrap()
        .into_first_result();
    let sname = row[0].get::<&str, _>(0usize);
    assert!(sname.is_some(), "SUSER_SNAME() returned NULL");
    eprintln!("AAD login as: {sname:?}");
}
