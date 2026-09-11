//! Live pooling regressions. Configure TEST_DB_PASSWORD and optionally
//! TEST_DB_HOST, TEST_DB_PORT, TEST_DB_USER, and TEST_DB_NAME.

use std::time::Duration;

use deadpool::managed::{Object, Timeouts};
use futures_util::StreamExt;
use mssql_tiberius_bridge::{AuthMethod, Client, Config, Error, Pool, RecyclingMethod, TdsManager};

fn test_config() -> Option<Config> {
    let Ok(password) = std::env::var("TEST_DB_PASSWORD") else {
        eprintln!("skipping live pooling test: TEST_DB_PASSWORD is not set");
        return None;
    };
    let mut config = Config::new();
    config
        .host(std::env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            std::env::var("TEST_DB_PORT")
                .map(|port| port.parse().expect("TEST_DB_PORT must be a valid port"))
                .unwrap_or(1433),
        )
        .database(std::env::var("TEST_DB_NAME").unwrap_or_else(|_| "master".into()))
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or_else(|_| "sa".into()),
            password,
        ))
        .trust_cert();
    Some(config)
}

async fn scalar(client: &mut Client, sql: &str) -> i32 {
    client
        .simple_query(sql)
        .await
        .expect("scalar query failed")
        .into_first_result()[0]
        .get(0)
        .expect("expected an int scalar")
}

async fn start_waiting_batch(client: &mut Client) {
    client
        .inner_mut()
        .execute(
            "SELECT 1; RAISERROR('flush before wait', 0, 1) WITH NOWAIT; \
             WAITFOR DELAY '00:00:05'"
                .into(),
            (),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn native_pool_reset_isolates_borrowers_on_the_same_connection() {
    let Some(config) = test_config() else {
        return;
    };
    let pool = TdsManager::create_pool(config, 1).unwrap();
    let mut conn = pool.get().await.unwrap();
    let original = conn
        .simple_query("SELECT @@SPID AS spid, DB_NAME() AS db, @@LOCK_TIMEOUT AS lock_timeout")
        .await
        .unwrap()
        .into_first_result();
    let spid = original[0].get::<i16, _>("spid").unwrap();
    let database = original[0].get::<&str, _>("db").unwrap().to_string();
    let lock_timeout = original[0].get::<i32, _>("lock_timeout").unwrap();
    let statement = conn.prepare("SELECT 42", &[]).await.unwrap();

    conn.simple_query(
        "USE tempdb; \
         CREATE TABLE #bridge_pool_reset (value int); \
         SET LOCK_TIMEOUT 1234; \
         SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; \
         BEGIN TRANSACTION; \
         INSERT INTO #bridge_pool_reset VALUES (42)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&mut conn, "SELECT @@TRANCOUNT").await, 1);
    drop(conn);

    let mut conn = pool.get().await.unwrap();
    assert_eq!(Object::metrics(&conn).recycle_count, 1);
    let rows = conn
        .simple_query(
            "SELECT @@SPID AS spid, DB_NAME() AS db, @@LOCK_TIMEOUT AS lock_timeout, \
             @@TRANCOUNT AS tran_count, \
             CASE WHEN OBJECT_ID('tempdb..#bridge_pool_reset') IS NULL THEN 1 ELSE 0 END AS cleared, \
             CAST(transaction_isolation_level AS int) AS isolation_level \
             FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        )
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows[0].get::<i16, _>("spid"), Some(spid));
    assert_eq!(rows[0].get::<&str, _>("db"), Some(database.as_str()));
    assert_eq!(rows[0].get::<i32, _>("lock_timeout"), Some(lock_timeout));
    assert_eq!(rows[0].get::<i32, _>("tran_count"), Some(0));
    assert_eq!(rows[0].get::<i32, _>("cleared"), Some(1));
    assert_eq!(rows[0].get::<i32, _>("isolation_level"), Some(2));
    assert!(matches!(
        statement.query(&mut conn, &[]).await,
        Err(Error::InvalidPreparedStatement)
    ));
}

#[tokio::test]
async fn reset_rejects_stale_prepared_handles_without_unpreparing_new_ones() {
    let Some(config) = test_config() else {
        return;
    };
    let mut client = Client::connect(&config).await.unwrap();
    let old = client.prepare("SELECT @P1", &[&0i32]).await.unwrap();
    client.reset_session().await.unwrap();

    assert!(matches!(
        old.query(&mut client, &[&1i32]).await,
        Err(Error::InvalidPreparedStatement)
    ));
    assert!(matches!(
        old.execute(&mut client, &[&1i32]).await,
        Err(Error::InvalidPreparedStatement)
    ));
    let fresh = client.prepare("SELECT @P1 + 1", &[&0i32]).await.unwrap();
    assert!(matches!(
        old.close(&mut client).await,
        Err(Error::InvalidPreparedStatement)
    ));
    let rows = fresh
        .query(&mut client, &[&41i32])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>(0), Some(42));
    fresh.close(&mut client).await.unwrap();
}

#[tokio::test]
async fn explicit_ping_recycling_preserves_legacy_session_state() {
    let Some(config) = test_config() else {
        return;
    };
    let manager = TdsManager::new(config).with_recycling_method(RecyclingMethod::Ping);
    let pool = Pool::builder(manager).max_size(1).build().unwrap();
    let mut conn = pool.get().await.unwrap();
    let statement = conn.prepare("SELECT 42", &[]).await.unwrap();
    conn.simple_query(
        "CREATE TABLE #bridge_pool_legacy (value int); \
         SET LOCK_TIMEOUT 1234; BEGIN TRANSACTION; \
         INSERT INTO #bridge_pool_legacy VALUES (7)",
    )
    .await
    .unwrap();
    drop(conn);

    let mut conn = pool.get().await.unwrap();
    assert_eq!(Object::metrics(&conn).recycle_count, 1);
    assert_eq!(scalar(&mut conn, "SELECT @@LOCK_TIMEOUT").await, 1234);
    assert_eq!(scalar(&mut conn, "SELECT @@TRANCOUNT").await, 1);
    assert_eq!(
        scalar(&mut conn, "SELECT value FROM #bridge_pool_legacy").await,
        7
    );
    let rows = statement
        .query(&mut conn, &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>(0), Some(42));
    conn.simple_query("ROLLBACK TRANSACTION").await.unwrap();
    statement.close(&mut conn).await.unwrap();
}

#[tokio::test]
async fn both_recycling_methods_discard_known_dead_connections() {
    let Some(config) = test_config() else {
        return;
    };
    for method in [RecyclingMethod::Reset, RecyclingMethod::Ping] {
        let manager = TdsManager::new(config.clone()).with_recycling_method(method);
        let pool = Pool::builder(manager).max_size(1).build().unwrap();
        let mut conn = pool.get().await.unwrap();
        assert!(!conn.is_connection_dead());
        conn.inner_mut().close_connection().await.unwrap();
        assert!(conn.is_connection_dead());
        assert!(matches!(
            conn.reset_session().await,
            Err(Error::Tds(mssql_tds::error::Error::ConnectionClosed(_)))
        ));
        drop(conn);

        let mut replacement = pool.get().await.unwrap();
        assert_eq!(Object::metrics(&replacement).recycle_count, 0);
        assert_eq!(scalar(&mut replacement, "SELECT 42").await, 42);
    }
}

#[tokio::test]
async fn reset_drains_an_abandoned_stream_before_arming_the_reset() {
    let Some(config) = test_config() else {
        return;
    };
    let mut client = Client::connect(&config).await.unwrap();
    let default_timeout = scalar(&mut client, "SELECT @@LOCK_TIMEOUT").await;
    let mut stream = client
        .simple_query_streamed("SELECT 1 UNION ALL SELECT 2; SET LOCK_TIMEOUT 1234; SELECT 3");
    assert!(stream.next().await.unwrap().is_ok());
    drop(stream);

    client.reset_session().await.unwrap();
    assert_eq!(
        scalar(&mut client, "SELECT @@LOCK_TIMEOUT").await,
        default_timeout
    );
    assert!(!client.inner_mut().reset_pending());
}

#[tokio::test]
async fn a_failed_reset_retires_the_connection() {
    let Some(config) = test_config() else {
        return;
    };
    let mut client = Client::connect(&config).await.unwrap();
    client
        .inner_mut()
        .execute("SELECT 1; THROW 50000, 'reset drain failure', 1".into(), ())
        .await
        .unwrap();
    assert!(client.reset_session().await.is_err());
    assert!(client.is_connection_dead());
}

#[tokio::test]
async fn cancelling_a_reset_retires_the_connection() {
    let Some(config) = test_config() else {
        return;
    };
    let mut client = Client::connect(&config).await.unwrap();
    start_waiting_batch(&mut client).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.reset_session())
            .await
            .is_err()
    );
    assert!(client.is_connection_dead());
}

#[tokio::test]
async fn pool_replaces_a_connection_when_reset_fails() {
    let Some(config) = test_config() else {
        return;
    };
    let pool = TdsManager::create_pool(config, 1).unwrap();
    let mut conn = pool.get().await.unwrap();
    conn.inner_mut()
        .execute(
            "SELECT 1; THROW 50000, 'recycle drain failure', 1".into(),
            (),
        )
        .await
        .unwrap();
    drop(conn);

    let mut replacement = pool.get().await.unwrap();
    assert_eq!(Object::metrics(&replacement).recycle_count, 0);
    assert_eq!(scalar(&mut replacement, "SELECT 42").await, 42);
}

#[tokio::test]
async fn pool_replaces_a_connection_when_reset_times_out() {
    let Some(config) = test_config() else {
        return;
    };
    let pool = Pool::builder(TdsManager::new(config))
        .max_size(1)
        .runtime(deadpool::Runtime::Tokio1)
        .timeouts(Timeouts {
            recycle: Some(Duration::from_millis(50)),
            ..Timeouts::default()
        })
        .build()
        .unwrap();
    let mut conn = pool.get().await.unwrap();
    start_waiting_batch(&mut conn).await;
    drop(conn);

    let mut replacement = tokio::time::timeout(Duration::from_secs(10), pool.get())
        .await
        .expect("checkout must complete after discarding the timed-out connection")
        .unwrap();
    assert_eq!(Object::metrics(&replacement).recycle_count, 0);
    assert_eq!(scalar(&mut replacement, "SELECT 42").await, 42);
}

#[tokio::test]
async fn convenience_pool_supports_checkout_timeouts() {
    let Some(config) = test_config() else {
        return;
    };
    let pool = TdsManager::create_pool(config, 1).unwrap();
    let conn = pool.get().await.unwrap();
    assert!(matches!(
        pool.timeout_get(&Timeouts::wait_millis(10)).await,
        Err(deadpool::managed::PoolError::Timeout(
            deadpool::managed::TimeoutType::Wait
        ))
    ));
    drop(conn);
    let mut conn = pool.get().await.unwrap();
    assert_eq!(scalar(&mut conn, "SELECT 42").await, 42);
}
