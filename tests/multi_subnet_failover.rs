//! MultiSubnetFailover integration test (issue #61).
//!
//! Real MSF only matters against an Always On listener whose DNS record
//! resolves to multiple IPs across subnets — we don't have one of those in
//! CI. What we *can* assert in CI is that enabling MSF against a normal
//! single-IP endpoint still connects and queries successfully (i.e., the
//! parallel-connect code path doesn't break the common case). This is the
//! same posture the .NET driver tests take for MSF in their bring-up suite.
//!
//! Reuses the standard `TEST_DB_*` env vars; skips when `TEST_DB_PASSWORD`
//! is unset.

use mssql_tiberius_bridge::{AuthMethod, Client, Config};

fn msf_config() -> Option<Config> {
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
        .trust_cert()
        .multi_subnet_failover(true);
    Some(cfg)
}

#[tokio::test]
async fn multi_subnet_failover_connects_to_single_endpoint() {
    let Some(cfg) = msf_config() else {
        eprintln!(
            "skipping multi_subnet_failover_connects_to_single_endpoint: TEST_DB_PASSWORD not set"
        );
        return;
    };
    assert!(cfg.is_multi_subnet_failover());

    let mut client = Client::connect(&cfg)
        .await
        .expect("MSF connect against single-IP endpoint should succeed");
    let rows = client
        .simple_query("SELECT 1 AS value")
        .await
        .expect("query failed")
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>("value"), Some(1));
}
