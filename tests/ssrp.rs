//! SSRP / SQL Browser instance lookup integration test (issue #14).
//!
//! Skipped unless `BRIDGE_SSRP_HOST` and `BRIDGE_SSRP_INSTANCE` are set,
//! because the standard MSSQL Docker images don't run the SQL Browser
//! service (UDP 1434).
//!
//! Example:
//!     BRIDGE_SSRP_HOST=mssql.lab \
//!     BRIDGE_SSRP_INSTANCE=SQLEXPRESS \
//!     TEST_DB_USER=sa TEST_DB_PASSWORD=... \
//!     cargo test --test ssrp -- --nocapture

use mssql_tiberius_bridge::{AuthMethod, Client, Config};

#[tokio::test]
async fn ssrp_named_instance_connect() {
    let Ok(host) = std::env::var("BRIDGE_SSRP_HOST") else {
        eprintln!("BRIDGE_SSRP_HOST not set, skipping");
        return;
    };
    let Ok(instance) = std::env::var("BRIDGE_SSRP_INSTANCE") else {
        eprintln!("BRIDGE_SSRP_INSTANCE not set, skipping");
        return;
    };
    let user = std::env::var("TEST_DB_USER").unwrap_or_else(|_| "sa".into());
    let password = std::env::var("TEST_DB_PASSWORD").expect("TEST_DB_PASSWORD required");
    let database = std::env::var("TEST_DB_NAME").unwrap_or_else(|_| "master".into());

    let mut cfg = Config::new();
    cfg.host(host)
        .instance_name(instance)
        .database(database)
        .authentication(AuthMethod::sql_server(user, password))
        .trust_cert();

    let mut client = Client::connect(&cfg)
        .await
        .expect("SSRP-resolved connect failed");
    let row = client
        .query("SELECT @@SERVERNAME", &[])
        .await
        .unwrap()
        .into_first_result();
    let server_name = row[0].get::<&str, _>(0usize);
    assert!(server_name.is_some());
    eprintln!("SSRP-resolved @@SERVERNAME: {:?}", server_name);
}
