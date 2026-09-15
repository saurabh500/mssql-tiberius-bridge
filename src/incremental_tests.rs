use super::*;
use crate::row::{BridgeRowWriter, RowSchema};
use mssql_tds::test_client_support::*;

fn client(inner: TdsClient) -> Client {
    Client {
        inner,
        send_string_parameters_as_unicode: true,
        prepared_session: Arc::new(()),
    }
}

fn writer(client: &Client) -> BridgeRowWriter {
    BridgeRowWriter::new(RowSchema::from_metadata(
        client.query_metadata().expect("current metadata"),
    ))
}

#[tokio::test]
async fn ping_recycling_rejects_pending_rows_and_trailing_results_without_io() {
    use deadpool::managed::{Manager, Metrics, RecycleError};
    let manager =
        crate::TdsManager::new(Config::new()).with_recycling_method(crate::RecyclingMethod::Ping);
    let mut client = client(tds_client_from_tokens(vec![
        col_metadata(int_columns(1)),
        done_more(),
        col_metadata(int_columns(2)),
        done_no_more(),
    ]));
    client.start_query("first", &[]).await.expect("start");
    for boundary in [false, true] {
        if boundary {
            let mut writer = writer(&client);
            assert!(!client.next_row_into(&mut writer).await.expect("boundary"));
        }
        for _ in 0..2 {
            assert!(
                client.has_pending_results(),
                "fixture must retain an open batch"
            );
            assert!(matches!(
                manager.recycle(&mut client, &Metrics::default()).await,
                Err(RecycleError::Message(_))
            ));
            assert!(client.has_pending_results(), "rejection must not drain");
            assert!(!client.is_connection_dead(), "pending does not mean dead");
        }
    }
    assert!(client.next_result().await.expect("unconsumed next result"));
    assert_eq!(client.query_metadata().expect("metadata").len(), 2);
    client.close_query().await.expect("close");
    for _ in 0..2 {
        manager
            .recycle(&mut client, &Metrics::default())
            .await
            .expect("healthy idle recycle");
    }
}

#[tokio::test]
async fn incremental_idle_and_closed_reads_are_explicitly_idempotent() {
    let mut client = client(tds_client_from_int_rows(vec![vec![42]]));
    let mut writer = BridgeRowWriter::new(RowSchema::from_metadata(&int_columns(1)));
    for _ in 0..2 {
        assert!(!client.next_row_into(&mut writer).await.expect("idle read"));
    }
    client.start_query("rows", &[]).await.expect("start");
    client.close_query().await.expect("close unread query");
    for _ in 0..2 {
        assert!(!client
            .next_row_into(&mut writer)
            .await
            .expect("closed read"));
        assert!(!client.next_result().await.expect("EOF"));
    }
    assert!(!client.is_connection_dead());
}

#[tokio::test]
async fn incremental_consecutive_refills_and_stable_eof() {
    for partial in [false, true] {
        let rows = (0..65).map(|n| vec![n, n + 1]).collect();
        let inner = if partial {
            tds_client_from_partial_int_rows(rows, 1)
        } else {
            tds_client_from_int_rows(rows)
        };
        let mut client = client(inner);
        client.query_metadata().expect_err("idle metadata");
        assert!(client.start_query("rows", &[]).await.expect("start"));
        let mut writer = writer(&client);
        let mut counts = Vec::new();
        let mut total = 0;
        loop {
            let mut count = 0;
            for _ in 0..32 {
                if !client.next_row_into(&mut writer).await.expect("read") {
                    break;
                }
                let row = writer.take_row();
                assert_eq!(row.get::<i32, _>(0), Some(total));
                assert_eq!(row.get::<i32, _>(1), Some(total + 1));
                total += 1;
                count += 1;
            }
            counts.push(count);
            if count < 32 {
                break;
            }
        }
        assert_eq!(counts, [32, 32, 1]);
        assert_eq!(total, 65);
        for _ in 0..2 {
            assert!(!client.next_row_into(&mut writer).await.expect("row EOF"));
            assert!(!client.next_result().await.expect("query EOF"));
            client.close_query().await.expect("idempotent close");
            assert!(!client.has_pending_results());
            assert!(!client.is_connection_dead());
            client.query_metadata().expect_err("EOF metadata");
        }
    }
}

#[tokio::test]
async fn incremental_empty_metadata_boundaries_trailing_statements_and_reuse() {
    let mut tokens = Vec::new();
    for _ in 0..2 {
        tokens.extend([
            done_more_with_count(3),
            col_metadata(int_columns(1)),
            done_more(),
            col_metadata(int_columns(2)),
            done_more(),
            done_no_more(),
        ]);
    }
    let mut client = client(tds_client_from_tokens(tokens));
    for _ in 0..2 {
        assert!(client
            .start_query("empty results", &[])
            .await
            .expect("start"));
        assert_eq!(client.query_metadata().expect("first metadata").len(), 1);
        let mut writer = writer(&client);
        for _ in 0..2 {
            assert!(!client.next_row_into(&mut writer).await.expect("empty"));
            assert!(
                client.has_pending_results(),
                "trailing results still pending"
            );
            assert!(
                client.query_metadata().is_err(),
                "no stale metadata at boundary"
            );
        }
        assert!(client.next_result().await.expect("second result"));
        assert_eq!(client.query_metadata().expect("second metadata").len(), 2);
        assert!(!client
            .next_row_into(&mut writer)
            .await
            .expect("empty second"));
        assert!(client.has_pending_results(), "trailing non-row result");
        assert!(!client.next_result().await.expect("query EOF"));
        assert!(!client.has_pending_results());
        assert!(!client.is_connection_dead());
    }
}

#[tokio::test]
async fn incremental_close_and_new_start_drain_unread_results() {
    for explicit_close in [false, true] {
        let mut client = client(tds_client_from_tokens(vec![
            col_metadata(int_columns(1)),
            done_more(),
            col_metadata(int_columns(2)),
            done_no_more(),
            col_metadata(int_columns(3)),
            done_no_more(),
        ]));
        assert!(client.start_query("first", &[]).await.expect("start"));
        if explicit_close {
            client.close_query().await.expect("close");
            client.query_metadata().expect_err("closed metadata");
            assert!(!client.has_pending_results());
        }
        assert!(client.start_query("next", &[]).await.expect("reuse"));
        assert_eq!(client.query_metadata().expect("fresh metadata").len(), 3);
        client.close_query().await.expect("finish");
        assert!(!client.is_connection_dead());
    }
}

#[tokio::test]
async fn incremental_unpolled_futures_preserve_healthy_active_query() {
    let mut client = client(tds_client_from_int_rows(vec![vec![42]]));
    drop(client.start_query("unpolled", &[]));
    drop(client.query_first("unpolled", &[]));
    assert!(!client.has_pending_results());
    client.start_query("actual", &[]).await.expect("start");
    let mut writer = writer(&client);
    drop(client.next_row_into(&mut writer));
    drop(client.next_result());
    drop(client.close_query());
    assert!(client.has_pending_results());
    assert!(!client.is_connection_dead());
    assert!(client
        .next_row_into(&mut writer)
        .await
        .expect("preserved row"));
    assert_eq!(writer.take_row().get::<i32, _>(0), Some(42));
    client.close_query().await.expect("close");
    assert!(!client.has_pending_results());
}

#[tokio::test]
async fn incremental_sql_errors_keep_health_but_failed_drains_retire() {
    for at_start in [false, true] {
        for broken in [false, true] {
            let mut tokens = Vec::new();
            if !at_start {
                tokens.extend([col_metadata(int_columns(1)), done_more()]);
            }
            tokens.push(sql_error(50000, 16, "scripted SQL error"));
            if !broken {
                tokens.extend([done_no_more(), col_metadata(int_columns(2)), done_no_more()]);
            }
            let mut client = client(tds_client_from_tokens(tokens));
            let result = if at_start {
                client.start_query("error", &[]).await.map(|_| ())
            } else {
                client
                    .start_query("rows then error", &[])
                    .await
                    .expect("start");
                client.close_query().await
            };
            assert!(matches!(
                result,
                Err(Error::Tds(crate::writer::TdsError::SqlServerError { .. }))
            ));
            assert_eq!(client.is_connection_dead(), broken);
            if !broken {
                client.close_query().await.expect("settle");
                assert!(!client.has_pending_results());
                assert!(client
                    .start_query("healthy reuse", &[])
                    .await
                    .expect("reuse"));
                assert_eq!(client.query_metadata().expect("metadata").len(), 2);
                client.close_query().await.expect("finish");
            }
        }
    }
}

#[tokio::test]
async fn incremental_transport_error_and_dead_checks() {
    let mut client = client(tds_client_from_tokens(vec![col_metadata(int_columns(1))]));
    client
        .start_query("truncated", &[])
        .await
        .expect("metadata");
    let mut writer = writer(&client);
    client
        .next_row_into(&mut writer)
        .await
        .expect_err("truncated row");
    // The token replay double does not mark transport EOF dead; draining does.
    // The wire tests exercise real transport failure without this limitation.
    client.close_query().await.expect_err("truncated drain");
    assert!(client.is_connection_dead());
    for _ in 0..2 {
        client.query_metadata().expect_err("dead metadata");
        client
            .start_query("never sent", &[])
            .await
            .expect_err("dead start");
        client
            .next_row_into(&mut writer)
            .await
            .expect_err("dead read");
        client.next_result().await.expect_err("dead advance");
        client.close_query().await.expect_err("dead close");
        client
            .query_first("never sent", &[])
            .await
            .expect_err("dead scalar");
    }
}

#[tokio::test]
async fn incremental_query_first_drains_and_does_not_skip_empty_first_set() {
    let mut populated = client(tds_client_from_int_rows(vec![vec![1], vec![2]]));
    let row = populated
        .query_first("two rows", &[])
        .await
        .expect("first")
        .expect("row");
    assert_eq!(row.get::<i32, _>(0), Some(1));
    assert!(!populated.has_pending_results());
    assert!(!populated.is_connection_dead());
    let mut empty = client(tds_client_from_tokens(vec![
        col_metadata(int_columns(1)),
        done_more(),
        col_metadata(int_columns(2)),
        done_no_more(),
    ]));
    assert!(empty
        .query_first("empty first", &[])
        .await
        .expect("first")
        .is_none());
    assert!(!empty.has_pending_results());
    let mut failed = client(tds_client_from_tokens(vec![
        col_metadata(int_columns(1)),
        done_more(),
        sql_error(50000, 16, "trailing error"),
        done_no_more(),
    ]));
    failed
        .query_first("trailing error", &[])
        .await
        .expect_err("trailing SQL error");
    assert!(!failed.has_pending_results());
    assert!(!failed.is_connection_dead());
}

#[tokio::test]
async fn incremental_no_row_query_and_parameterized_start() {
    let mut client = client(tds_client_from_tokens(vec![
        done_no_more(),
        col_metadata(int_columns(1)),
        done_no_more(),
    ]));
    assert!(!client.start_query("no rowset", &[]).await.expect("start"));
    client.query_metadata().expect_err("no rowset metadata");
    assert!(!client.has_pending_results());
    assert!(client
        .start_query("SELECT @P1", &[&42i32])
        .await
        .expect("RPC"));
    assert_eq!(client.query_metadata().expect("RPC metadata").len(), 1);
    client.close_query().await.expect("close");
}
