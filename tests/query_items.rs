//! Ordinary-API regressions against a controlled loopback TDS peer, not SQL Server.
use std::time::Duration;

use futures_util::{poll, Stream, StreamExt, TryStreamExt};
use mssql_tiberius_bridge::{Client, ColumnType, Config, EncryptionLevel, QueryItem};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(5);
const DONE: &[u8] = &[0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
const DONE_MORE: &[u8] = &[0xfd, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
const DONE_IN_PROC_MORE: &[u8] = &[0xff, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
const DONE_PROC: &[u8] = &[0xfe, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

#[tokio::test]
async fn fixed_char_binary_and_smallmoney_keep_empty_and_row_metadata() {
    let (mut client, mut peer) = connect().await;
    for (type_info, value, expected, length) in [
        (
            vec![0xaf, 2, 0, 9, 4, 0xd0, 0, 52],
            vec![2, 0, b'a', b'b'],
            ColumnType::Char,
            2,
        ),
        (
            vec![0xad, 2, 0],
            vec![2, 0, 0x12, 0x34],
            ColumnType::Binary,
            2,
        ),
        (
            vec![0x7a],
            12345i32.to_le_bytes().to_vec(),
            ColumnType::Money4,
            4,
        ),
    ] {
        for empty in [true, false] {
            let mut body = metadata("value", &type_info);
            if !empty {
                body.push(0xd1);
                body.extend(&value);
            }
            body.extend(DONE);
            let server = async {
                request(&mut peer, 1).await;
                reply(&mut peer, &body, true).await;
            };
            let mut stream = client.simple_query_items("SELECT fixture");
            let (item, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
                .await
                .expect("metadata deadline");
            let meta = match item.expect("item").expect("metadata") {
                QueryItem::Metadata(meta) => Some(meta),
                QueryItem::Row(_) => None,
            }
            .expect("metadata");
            let column = meta.columns().first().expect("column");
            assert_eq!(column.column_type(), expected);
            assert_eq!(column.byte_length(), length);
            assert!(column.nullable());
            assert_eq!(column.precision(), None);
            assert_eq!(meta.result_index(), 0);
            if !empty {
                let row = match next(&mut stream).await.expect("item").expect("row") {
                    QueryItem::Row(row) => Some(row),
                    QueryItem::Metadata(_) => None,
                }
                .expect("row");
                let row_column = row.columns().first().expect("row column");
                assert_eq!(row_column.column_type(), expected);
                assert_eq!(row_column.byte_length(), length);
                assert_eq!(row_column.scale(), column.scale());
                match expected {
                    ColumnType::Char => assert_eq!(row.get::<&str, _>(0), Some("ab")),
                    ColumnType::Binary => {
                        assert_eq!(row.get::<&[u8], _>(0), Some([0x12, 0x34].as_slice()))
                    }
                    _ => assert!(matches!(
                        row.raw_value(0),
                        Some(mssql_tiberius_bridge::ColumnValues::SmallMoney(_))
                    )),
                }
            }
            assert!(next(&mut stream).await.is_none());
            drop(stream);
            healthy_reuse(&mut client, &mut peer).await;
        }
    }
}

// Handshake helpers follow the existing incremental_wire fixture in PR #124.
async fn request(peer: &mut TcpStream, kind: u8) {
    loop {
        let mut header = [0; 8];
        peer.read_exact(&mut header).await.expect("request header");
        assert_eq!(header[0], kind);
        let len = usize::from(u16::from_be_bytes([header[2], header[3]]));
        assert!(len >= 8);
        let mut payload = vec![0; len - 8];
        peer.read_exact(&mut payload)
            .await
            .expect("request payload");
        if header[1] & 1 != 0 {
            break;
        }
    }
}

async fn reply(peer: &mut TcpStream, payload: &[u8], final_packet: bool) {
    let len = u16::try_from(payload.len() + 8)
        .expect("small fixture packet")
        .to_be_bytes();
    peer.write_all(&[4, u8::from(final_packet), len[0], len[1], 0, 0, 1, 0])
        .await
        .expect("reply header");
    peer.write_all(payload).await.expect("reply payload");
}

fn env_change(kind: u8, new: &[u8], len: u8) -> Vec<u8> {
    let mut payload = vec![kind, len];
    payload.extend(new);
    payload.push(0);
    let mut token = vec![0xe3];
    token.extend(
        u16::try_from(payload.len())
            .expect("ENVCHANGE")
            .to_le_bytes(),
    );
    token.extend(payload);
    token
}

async fn connect() -> (Client, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(listener.local_addr().expect("address").port())
        .encryption(EncryptionLevel::Off);
    let server = async {
        let (mut peer, _) = listener.accept().await.expect("accept");
        request(&mut peer, 0x12).await;
        // VERSION and ENCRYPT_NOT_SUP for the loopback-only peer.
        reply(
            &mut peer,
            &[0, 0, 11, 0, 6, 1, 0, 17, 0, 1, 0xff, 16, 0, 0, 0, 0, 0, 2],
            true,
        )
        .await;
        request(&mut peer, 0x10).await;
        let mut login = vec![0xad, 10, 0, 1, 0x74, 0, 0, 4, 0, 16, 0, 0, 1];
        login.extend(env_change(1, b"m\0a\0s\0t\0e\0r\0", 6));
        login.extend(env_change(7, &[9, 4, 0xd0, 0, 52], 5));
        login.extend([
            0xe3, 19, 0, 4, 4, b'4', 0, b'0', 0, b'9', 0, b'6', 0, 4, b'4', 0, b'0', 0, b'9', 0,
            b'6', 0,
        ]);
        login.extend(DONE);
        reply(&mut peer, &login, true).await;
        peer
    };
    let (client, peer) = timeout(DEADLINE, async {
        tokio::join!(Client::connect(&config), server)
    })
    .await
    .expect("login deadline");
    (client.expect("login"), peer)
}

fn metadata(name: &str, type_info: &[u8]) -> Vec<u8> {
    let mut token = vec![0x81, 1, 0, 0, 0, 0, 0, 1, 0];
    token.extend(type_info);
    token.push(u8::try_from(name.len()).expect("short ASCII name"));
    token.extend(name.encode_utf16().flat_map(u16::to_le_bytes));
    token
}

fn int_result(value: i32) -> Vec<u8> {
    let mut body = metadata("n", &[0x38]);
    body.push(0xd1);
    body.extend(value.to_le_bytes());
    body.extend(DONE);
    body
}

async fn next<S: Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    timeout(DEADLINE, stream.next())
        .await
        .expect("item deadline")
}

fn assert_metadata(item: QueryItem, index: usize, name: &str, ty: ColumnType) {
    let meta = match item {
        QueryItem::Metadata(meta) => Some(meta),
        QueryItem::Row(_) => None,
    }
    .expect("expected metadata before rows");
    assert_eq!(meta.result_index(), index);
    assert_eq!(meta.columns().len(), 1);
    let column = meta.columns().first().expect("column");
    assert_eq!(column.name(), name);
    assert_eq!(column.column_type(), ty);
    assert!(column.nullable());
}

async fn healthy_reuse(client: &mut Client, peer: &mut TcpStream) {
    let server = async {
        request(peer, 1).await;
        reply(peer, &int_result(43), true).await;
    };
    let (result, ()) = timeout(DEADLINE, async {
        tokio::join!(client.simple_query("SELECT 43 AS n"), server)
    })
    .await
    .expect("reuse deadline");
    assert_eq!(
        result
            .expect("reuse")
            .into_first_result()
            .first()
            .expect("row")
            .get::<i32, _>("n"),
        Some(43)
    );
    assert!(!client.is_connection_dead());
}

#[tokio::test]
async fn empty_selects_keep_distinct_names_types_and_indexes() {
    let (mut client, mut peer) = connect().await;
    for parameterized in [false, true] {
        // Empty int, nonempty int, empty bigint, nonempty int: no stale schema.
        let boundary = if parameterized {
            DONE_IN_PROC_MORE
        } else {
            DONE_MORE
        };
        let mut body = metadata("empty_int", &[0x38]);
        body.extend(boundary);
        body.extend(metadata("value", &[0x38]));
        body.extend([0xd1, 7, 0, 0, 0]);
        body.extend(boundary);
        body.extend(metadata("empty_bigint", &[0x7f]));
        body.extend(boundary);
        body.extend(metadata("last", &[0x38]));
        body.extend([0xd1, 8, 0, 0, 0]);
        if parameterized {
            body.extend(DONE_IN_PROC_MORE);
            body.extend([0x79, 0, 0, 0, 0]); // RETURNSTATUS
            body.extend(DONE_PROC);
        } else {
            body.extend(DONE);
        }
        let server = async {
            request(&mut peer, if parameterized { 3 } else { 1 }).await;
            reply(&mut peer, &body, true).await;
        };
        let mut stream = if parameterized {
            client.query_items(
                "SELECT @P1 WHERE 1=0; SELECT 7; SELECT CAST(1 AS bigint) WHERE 1=0; SELECT 8",
                &[&1i32],
            )
        } else {
            client.simple_query_items(
                "SELECT 1 WHERE 1=0; SELECT 7; SELECT CAST(1 AS bigint) WHERE 1=0; SELECT 8",
            )
        };
        let (first, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
            .await
            .expect("first metadata deadline");
        assert_metadata(
            first.expect("item").expect("metadata"),
            0,
            "empty_int",
            ColumnType::Int4,
        );
        let mut retained = Vec::new();
        for (index, name, value) in [(1, "value", 7), (3, "last", 8)] {
            assert_metadata(
                next(&mut stream).await.expect("item").expect("metadata"),
                index,
                name,
                ColumnType::Int4,
            );
            let row = match next(&mut stream).await.expect("item").expect("row") {
                QueryItem::Row(row) => Some(row),
                QueryItem::Metadata(_) => None,
            }
            .expect("expected row");
            assert_eq!(row.try_get::<i32, _>(name).expect("value"), Some(value));
            assert_eq!(row.columns().first().expect("column").name(), name);
            retained.push(row);
            if index == 1 {
                assert_metadata(
                    next(&mut stream).await.expect("item").expect("empty"),
                    2,
                    "empty_bigint",
                    ColumnType::Int8,
                );
            }
        }
        assert!(next(&mut stream).await.is_none());
        assert!(next(&mut stream).await.is_none());
        drop(stream);
        assert_eq!(
            retained
                .first()
                .expect("retained row")
                .get::<i32, _>("value"),
            Some(7)
        );
        healthy_reuse(&mut client, &mut peer).await;
    }
}

#[tokio::test]
async fn independent_empty_queries_and_no_row_batches_are_distinguishable() {
    let (mut client, mut peer) = connect().await;
    for (name, ty, expected) in [("a", 0x38, ColumnType::Int4), ("b", 0x7f, ColumnType::Int8)] {
        let mut body = metadata(name, &[ty]);
        body.extend(DONE);
        let server = async {
            request(&mut peer, 1).await;
            reply(&mut peer, &body, true).await;
        };
        let mut items = client.simple_query_items(format!(
            "SELECT CAST(1 AS {}) AS {name} WHERE 1=0",
            if ty == 0x38 { "int" } else { "bigint" }
        ));
        let (item, ()) = timeout(DEADLINE, async { tokio::join!(items.next(), server) })
            .await
            .expect("empty deadline");
        assert_metadata(item.expect("item").expect("metadata"), 0, name, expected);
        assert!(next(&mut items).await.is_none());
    }
    let mut body = DONE_MORE.to_vec();
    // DONE_COUNT with 5 affected rows, followed by an empty varchar rowset.
    body.extend([0xfd, 0x11, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0]);
    for has_columns in [false, true] {
        let mut response = body.clone();
        if has_columns {
            response.extend(metadata("text", &[0xa7, 12, 0, 9, 4, 0xd0, 0, 52]));
        }
        response.extend(DONE);
        let server = async {
            request(&mut peer, 1).await;
            reply(&mut peer, &response, true).await;
        };
        let query = client.simple_query_items("SET NOCOUNT OFF; UPDATE fixture SET n=1");
        let (items, ()) = timeout(DEADLINE, async {
            tokio::join!(query.try_collect::<Vec<_>>(), server)
        })
        .await
        .expect("batch deadline");
        let items = items.expect("batch");
        assert_eq!(items.len(), usize::from(has_columns));
        if let Some(QueryItem::Metadata(meta)) = items.first() {
            let col = meta.columns().first().expect("column");
            assert_eq!(meta.result_index(), 0);
            assert_eq!(
                (col.name(), col.column_type(), col.byte_length()),
                ("text", ColumnType::Varchar, 12)
            );
            assert_eq!((col.precision(), col.scale()), (None, None));
        }
    }
}

#[tokio::test]
async fn metadata_and_rows_arrive_before_peer_releases_the_rest() {
    let (mut client, mut peer) = connect().await;
    for _ in 0..2 {
        let mut stream = client.simple_query_items("SELECT 42 AS n");
        let server = async {
            request(&mut peer, 1).await;
            // Deliberately no row token, row body, DONE, or final packet.
            reply(&mut peer, &metadata("n", &[0x38]), false).await;
        };
        let (item, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
            .await
            .expect("metadata must arrive while rows are withheld");
        assert_metadata(
            item.expect("item").expect("metadata"),
            0,
            "n",
            ColumnType::Int4,
        );
        assert!(poll!(stream.next()).is_pending());
        assert!(
            timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err(),
            "connected silent peer must block, not return EOF"
        );
        reply(&mut peer, &[0xd1, 42, 0, 0, 0], false).await;
        let row = match next(&mut stream).await.expect("item").expect("row") {
            QueryItem::Row(row) => Some(row),
            QueryItem::Metadata(_) => None,
        }
        .expect("expected row before EOF");
        assert_eq!(row.get::<i32, _>("n"), Some(42));
        reply(&mut peer, DONE, true).await;
        assert!(next(&mut stream).await.is_none());
        assert!(next(&mut stream).await.is_none());
        drop(stream);
        healthy_reuse(&mut client, &mut peer).await;
    }
}

#[tokio::test]
async fn dropping_at_metadata_or_row_drains_on_next_query() {
    let (mut client, mut peer) = connect().await;
    drop(client.simple_query_items("never sent"));
    for read_row in [false, true] {
        let mut stream = client.simple_query_items("SELECT 42 AS n");
        let server = async {
            request(&mut peer, 1).await;
            reply(&mut peer, &metadata("n", &[0x38]), false).await;
        };
        let (item, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
            .await
            .expect("metadata deadline");
        let retained = match item.expect("item").expect("metadata") {
            QueryItem::Metadata(meta) => Some(meta),
            QueryItem::Row(_) => None,
        }
        .expect("metadata");
        reply(&mut peer, &[0xd1, 42, 0, 0, 0], false).await;
        if read_row {
            assert!(matches!(
                next(&mut stream).await,
                Some(Ok(QueryItem::Row(_)))
            ));
        }
        drop(stream);
        assert_eq!(retained.columns().first().expect("retained").name(), "n");
        client.ping().await.expect("cached ping without DONE");
        assert!(!client.is_connection_dead());
        reply(&mut peer, DONE, true).await;
        healthy_reuse(&mut client, &mut peer).await;
    }
}

#[tokio::test]
async fn dropping_stream_during_pending_start_row_or_advance_retires() {
    for phase in 0..3 {
        let (mut client, mut peer) = connect().await;
        let mut stream = client.simple_query_items("SELECT 42 AS n");
        if phase > 0 {
            let server = async {
                request(&mut peer, 1).await;
                reply(&mut peer, &metadata("n", &[0x38]), false).await;
            };
            let (item, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
                .await
                .expect("metadata deadline");
            assert!(matches!(item, Some(Ok(QueryItem::Metadata(_)))));
        }
        if phase == 2 {
            reply(&mut peer, DONE_MORE, false).await;
        }
        assert!(poll!(stream.next()).is_pending());
        timeout(Duration::from_millis(20), stream.next())
            .await
            .expect_err("connected peer must remain pending");
        drop(stream);
        assert!(client.is_connection_dead());
        assert!(client.ping().await.is_err());
        next(&mut client.simple_query_items("must not send"))
            .await
            .expect("error")
            .expect_err("dead client must reject query");
        // Keep the peer connected throughout: EOF cannot stand in for pending I/O.
        drop(peer);
    }
}

fn sql_error() -> Vec<u8> {
    let message = "fixture conversion error";
    let mut payload = 50000u32.to_le_bytes().to_vec();
    payload.extend([1, 16]);
    payload.extend(u16::try_from(message.len()).expect("message").to_le_bytes());
    payload.extend(message.encode_utf16().flat_map(u16::to_le_bytes));
    payload.extend([0, 0]);
    payload.extend(1u32.to_le_bytes());
    let mut token = vec![0xaa];
    token.extend(
        u16::try_from(payload.len())
            .expect("error token")
            .to_le_bytes(),
    );
    token.extend(payload);
    token
}

#[tokio::test]
async fn initial_and_trailing_sql_errors_are_not_successful_eof() {
    let (mut client, mut peer) = connect().await;
    for phase in 0..3 {
        let mut body = Vec::new();
        if phase > 0 {
            body.extend(metadata("n", &[0x38]));
            if phase == 2 {
                body.extend([0xd1, 42, 0, 0, 0]);
            }
            body.extend(DONE_MORE);
        }
        body.extend(sql_error());
        body.extend(DONE);
        let server = async {
            request(&mut peer, 1).await;
            reply(&mut peer, &body, true).await;
        };
        let mut stream =
            client.simple_query_items("SELECT fixture; THROW 50000, 'fixture conversion error', 1");
        let (first, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
            .await
            .expect("error deadline");
        let error = if phase == 0 {
            first.expect("initial error")
        } else {
            assert_metadata(
                first.expect("item").expect("metadata"),
                0,
                "n",
                ColumnType::Int4,
            );
            if phase == 2 {
                assert!(matches!(
                    next(&mut stream).await,
                    Some(Ok(QueryItem::Row(_)))
                ));
            }
            next(&mut stream).await.expect("trailing error")
        };
        assert!(error
            .expect_err("SQL error")
            .to_string()
            .contains("fixture conversion error"));
        assert!(next(&mut stream).await.is_none());
        assert!(next(&mut stream).await.is_none());
        drop(stream);
        healthy_reuse(&mut client, &mut peer).await;
    }
}

#[tokio::test]
async fn existing_row_streams_still_flatten_empty_results() {
    let (mut client, mut peer) = connect().await;
    for parameterized in [false, true] {
        let mut body = metadata("empty", &[0x7f]);
        body.extend(DONE_MORE);
        body.extend(int_result(7));
        let server = async {
            request(&mut peer, if parameterized { 3 } else { 1 }).await;
            reply(&mut peer, &body, true).await;
        };
        let stream = if parameterized {
            client.query_streamed("SELECT @P1", &[&7i32])
        } else {
            client.simple_query_streamed("SELECT 7")
        };
        let (rows, ()) = timeout(DEADLINE, async {
            tokio::join!(stream.try_collect::<Vec<_>>(), server)
        })
        .await
        .expect("rows deadline");
        let rows = rows.expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.first().expect("row").get::<i32, _>("n"), Some(7));
        healthy_reuse(&mut client, &mut peer).await;
    }
}

#[tokio::test]
async fn partial_row_eof_is_an_error_not_a_row() {
    let (mut client, mut peer) = connect().await;
    let mut stream = client.simple_query_items("SELECT 42 AS n");
    let server = async {
        request(&mut peer, 1).await;
        let mut body = metadata("n", &[0x38]);
        body.extend([0xd1, 42, 0]); // Only half the four-byte integer.
        reply(&mut peer, &body, false).await;
    };
    let (item, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
        .await
        .expect("metadata deadline");
    assert_metadata(
        item.expect("item").expect("metadata"),
        0,
        "n",
        ColumnType::Int4,
    );
    drop(peer);
    next(&mut stream)
        .await
        .expect("error item")
        .expect_err("truncated row");
    assert!(next(&mut stream).await.is_none());
    drop(stream);
    assert!(client.is_connection_dead());
}

#[tokio::test]
async fn row_only_wrapper_preserves_pending_cancellation() {
    for parameterized in [false, true] {
        let (mut client, mut peer) = connect().await;
        let mut stream = if parameterized {
            client.query_streamed("SELECT @P1", &[&42i32])
        } else {
            client.simple_query_streamed("SELECT 42")
        };
        let server = async {
            request(&mut peer, if parameterized { 3 } else { 1 }).await;
            let mut body = metadata("n", &[0x38]);
            body.extend([0xd1, 42, 0, 0, 0]);
            reply(&mut peer, &body, false).await;
        };
        let (row, ()) = timeout(DEADLINE, async { tokio::join!(stream.next(), server) })
            .await
            .expect("row deadline");
        assert_eq!(row.expect("item").expect("row").get::<i32, _>(0), Some(42));
        assert!(poll!(stream.next()).is_pending());
        timeout(Duration::from_millis(20), stream.next())
            .await
            .expect_err("peer must remain pending");
        drop(stream);
        assert!(client.is_connection_dead());
        drop(peer);
    }
}
