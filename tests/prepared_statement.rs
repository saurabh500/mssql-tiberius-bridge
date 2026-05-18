//! Integration test for `Client::prepare` + `PreparedStatement` (issue #56).
//!
//! Exercises the full sp_prepare / sp_execute / sp_unprepare round-trip
//! against a live SQL Server. Skipped unless `TEST_DB_PASSWORD` is set.

use mssql_tiberius_bridge::{AuthMethod, Client, Config};

fn test_config() -> Config {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping prepared-statement test");
            std::process::exit(0);
        }
    };
    let mut cfg = Config::new();
    cfg.host(std::env::var("TEST_DB_HOST").unwrap_or("localhost".into()))
        .port(
            std::env::var("TEST_DB_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1433),
        )
        .database(std::env::var("TEST_DB_NAME").unwrap_or("master".into()))
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or("sa".into()),
            password,
        ))
        .trust_cert();
    cfg
}

#[tokio::test]
#[ignore = "Blocked on upstream mssql-rs#5: execute_sp_prepare attaches user named_params to the RPC, causing sp_prepare to return NULL handle. Re-enable when fixed."]
async fn prepare_select_arithmetic_runs_many_times_with_single_plan() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect");

    let stmt = client
        .prepare("SELECT @P1 + @P2 AS s", &[&0i32, &0i32])
        .await
        .expect("prepare");
    assert!(
        stmt.handle() > 0,
        "sp_prepare should return positive handle"
    );
    assert_eq!(stmt.sql(), "SELECT @P1 + @P2 AS s");

    for (a, b, expected) in [
        (1i32, 2i32, 3i32),
        (10, 20, 30),
        (-5, 5, 0),
        (100, 200, 300),
    ] {
        let rows = stmt
            .query(&mut client, &[&a, &b])
            .await
            .expect("query_prepared")
            .into_first_result();
        assert_eq!(rows.len(), 1);
        let sum: i32 = rows[0].get("s").expect("s column");
        assert_eq!(sum, expected, "{a} + {b} should be {expected}");
    }

    stmt.close(&mut client).await.expect("sp_unprepare");

    // Connection should still be usable after unprepare.
    let rows = client
        .simple_query("SELECT 1 AS one")
        .await
        .expect("simple_query after unprepare")
        .into_first_result();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>("one"), Some(1));
}

#[tokio::test]
#[ignore = "Blocked on upstream mssql-rs#5: execute_sp_prepare attaches user named_params to the RPC, causing sp_prepare to return NULL handle. Re-enable when fixed."]
async fn prepare_with_string_param_and_multiple_executions() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect");

    let stmt = client
        .prepare("SELECT @P1 AS msg, LEN(@P1) AS n", &[&"sample"])
        .await
        .expect("prepare");

    for s in ["hello", "world", "a longer phrase"] {
        let rows = stmt
            .query(&mut client, &[&s])
            .await
            .expect("query_prepared")
            .into_first_result();
        let msg: &str = rows[0].get("msg").unwrap();
        let n: i32 = rows[0].get("n").unwrap();
        assert_eq!(msg, s);
        assert_eq!(n as usize, s.len());
    }

    stmt.close(&mut client).await.expect("sp_unprepare");
}

#[tokio::test]
async fn prepare_with_no_params_works() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect");

    let stmt = client
        .prepare("SELECT 42 AS answer", &[])
        .await
        .expect("prepare no-params");

    let rows = stmt
        .query(&mut client, &[])
        .await
        .expect("query no-params")
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>("answer"), Some(42));

    stmt.close(&mut client).await.expect("close");
}

#[tokio::test]
#[ignore = "Blocked on upstream mssql-rs#5: execute_sp_prepare attaches user named_params to the RPC, causing sp_prepare to return NULL handle. Re-enable when fixed."]
async fn prepared_execute_against_dml() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect");

    // Use a temp table so we don't litter master.
    client
        .simple_query(
            "CREATE TABLE #t_prep_exec (id INT NOT NULL PRIMARY KEY, name NVARCHAR(64) NOT NULL)",
        )
        .await
        .expect("create temp table");

    let stmt = client
        .prepare(
            "INSERT INTO #t_prep_exec (id, name) VALUES (@P1, @P2)",
            &[&0i32, &""],
        )
        .await
        .expect("prepare INSERT");

    for (id, name) in [(1i32, "Alice"), (2, "Bob"), (3, "Carol")] {
        stmt.execute(&mut client, &[&id, &name])
            .await
            .expect("execute_prepared");
    }

    stmt.close(&mut client).await.expect("close");

    let rows = client
        .simple_query("SELECT COUNT(*) AS c FROM #t_prep_exec")
        .await
        .expect("count")
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>("c"), Some(3));
}
