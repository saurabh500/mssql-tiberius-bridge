//! Local, valid TDS response fixtures. The peer stays connected when silent.
use std::future::Future;
use std::time::Duration;

use futures_util::{poll, TryStreamExt};
use mssql_tds::datatypes::row_writer::DefaultRowWriter;
use mssql_tiberius_bridge::{Client, ColumnValues, Config, EncryptionLevel, Error, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(5);
const DONE: &[u8] = &[0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
const DONE_MORE: &[u8] = &[0xfd, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
const METADATA: &[u8] = &[0x81, 1, 0, 0, 0, 0, 0, 0, 0, 0x38, 1, b'n', 0];

#[tokio::test]
async fn connection_retry_count_controls_observed_attempts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(listener.local_addr().expect("address").port())
        .encryption(EncryptionLevel::Off);
    for retry_count in [0, 1, 0] {
        config.connect_retry_count(retry_count);
        let mut attempts = 0;
        let server = async {
            loop {
                let (mut peer, _) = listener.accept().await.expect("connection attempt");
                attempts += 1;
                request(&mut peer, 0x12).await;
                // EOF during prelogin is a transient native connection failure.
                drop(peer);
            }
        };
        let result = timeout(Duration::from_secs(20), async {
            tokio::select! {
                result = Client::connect(&config) => result,
                () = server => Err(Error::Conversion("fixture unexpectedly stopped".into())),
            }
        })
        .await
        .expect("bounded connection attempts");
        assert!(matches!(result, Err(Error::Tds(_))));
        assert_eq!(
            attempts,
            retry_count + 1,
            "count accepted sockets, not elapsed time"
        );
    }
}

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
            .expect("small ENVCHANGE")
            .to_le_bytes(),
    );
    token.extend(payload);
    token
}

async fn connect() -> (Client, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener");
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(listener.local_addr().expect("address").port())
        .encryption(EncryptionLevel::Off);
    let server = async {
        let (mut peer, _) = listener.accept().await.expect("accept");
        request(&mut peer, 0x12).await;
        // VERSION and ENCRYPT_NOT_SUP, for this loopback-only test peer.
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
        // Packet-size ENVCHANGE requires a numeric old value as well.
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
    .expect("mock login deadline");
    (client.expect("mock login"), peer)
}

async fn start(client: &mut Client, peer: &mut TcpStream, response: &[u8]) {
    let response = async {
        request(peer, 1).await;
        reply(peer, response, false).await;
    };
    let (result, ()) = timeout(DEADLINE, async {
        tokio::join!(client.start_query("SELECT fixture", &[]), response)
    })
    .await
    .expect("start deadline");
    assert!(result.expect("start with metadata"));
}

async fn cancel_pending<T>(future: impl Future<Output = T>) {
    let mut future = Box::pin(future);
    assert!(
        poll!(future.as_mut()).is_pending(),
        "must reach pending I/O"
    );
    assert!(
        timeout(Duration::from_millis(20), future.as_mut())
            .await
            .is_err(),
        "silent connected peer must not act like EOF"
    );
    drop(future);
}

async fn rejected<T>(future: impl Future<Output = Result<T>>) {
    let mut future = Box::pin(future);
    assert!(matches!(
        poll!(future.as_mut()),
        std::task::Poll::Ready(Err(Error::Tds(
            mssql_tiberius_bridge::writer::TdsError::ConnectionClosed(_)
        )))
    ));
}

async fn assert_dead(client: &mut Client) {
    assert!(client.is_connection_dead());
    let mut writer = DefaultRowWriter::new(1);
    rejected(client.start_query("not sent", &[])).await;
    rejected(client.next_row_into(&mut writer)).await;
    rejected(client.next_result()).await;
    rejected(client.close_query()).await;
    rejected(client.query_first("not sent", &[])).await;
    client.query_metadata().expect_err("dead metadata");
}

#[tokio::test]
async fn pending_start_read_advance_close_and_scalar_retire() {
    for phase in 0..5 {
        let (mut client, mut peer) = connect().await;
        if (1..=3).contains(&phase) {
            start(&mut client, &mut peer, METADATA).await;
        }
        match phase {
            0 => cancel_pending(client.start_query("silent start", &[])).await,
            1 => {
                let mut writer = DefaultRowWriter::new(1);
                cancel_pending(client.next_row_into(&mut writer)).await;
                assert!(writer.take_row().is_empty());
            }
            2 => {
                reply(&mut peer, DONE_MORE, false).await;
                let mut writer = DefaultRowWriter::new(1);
                assert!(!timeout(DEADLINE, client.next_row_into(&mut writer))
                    .await
                    .expect("boundary deadline")
                    .expect("boundary"));
                assert!(client.has_pending_results());
                cancel_pending(client.next_result()).await;
            }
            3 => cancel_pending(client.close_query()).await,
            _ => cancel_pending(client.query_first("silent scalar", &[])).await,
        }
        assert_dead(&mut client).await;
        // Keep peer alive through assertions: no fake EOF can rescue a pending read.
        drop(peer);
    }
}

#[tokio::test]
async fn real_transport_eof_retires_without_publishing_partial_row() {
    let (mut client, mut peer) = connect().await;
    start(&mut client, &mut peer, METADATA).await;
    reply(&mut peer, &[0xd1, 42, 0], false).await; // incomplete int row
    drop(peer);
    let mut writer = DefaultRowWriter::new(1);
    timeout(DEADLINE, client.next_row_into(&mut writer))
        .await
        .expect("EOF deadline")
        .expect_err("truncated row");
    assert_dead(&mut client).await;
}

#[tokio::test]
async fn healthy_wire_rows_empty_results_and_repeated_reuse() {
    let (mut client, mut peer) = connect().await;
    for _ in 0..2 {
        start(&mut client, &mut peer, METADATA).await;
        let mut body = vec![0xd1, 42, 0, 0, 0];
        body.extend(DONE_MORE);
        body.extend(METADATA);
        body.extend(DONE);
        reply(&mut peer, &body, true).await;
        let mut writer = DefaultRowWriter::new(1);
        assert!(client.next_row_into(&mut writer).await.expect("row"));
        assert!(matches!(
            writer.take_row().as_slice(),
            [ColumnValues::Int(42)]
        ));
        assert!(!client.next_row_into(&mut writer).await.expect("boundary"));
        assert!(client.has_pending_results());
        assert!(client.next_result().await.expect("empty second rowset"));
        assert_eq!(client.query_metadata().expect("empty metadata").len(), 1);
        assert!(!client.next_row_into(&mut writer).await.expect("empty"));
        assert!(!client.next_result().await.expect("EOF"));
        client.close_query().await.expect("close");
        assert!(!client.has_pending_results());
        assert!(!client.is_connection_dead());
    }
}

#[tokio::test]
async fn next_result_drains_unread_rows_before_advancing() {
    let (mut client, mut peer) = connect().await;
    for read_first in [false, true] {
        start(&mut client, &mut peer, METADATA).await;
        let mut body = vec![0xd1, 10, 0, 0, 0, 0xd1, 11, 0, 0, 0];
        body.extend(DONE_MORE);
        body.extend(METADATA);
        body.extend([0xd1, 20, 0, 0, 0]);
        body.extend(DONE);
        reply(&mut peer, &body, true).await;
        let mut writer = DefaultRowWriter::new(1);
        if read_first {
            assert!(client.next_row_into(&mut writer).await.expect("first row"));
            assert!(matches!(
                writer.take_row().as_slice(),
                [ColumnValues::Int(10)]
            ));
        }
        assert!(timeout(DEADLINE, client.next_result())
            .await
            .expect("advance deadline")
            .expect("second rowset"));
        assert!(client
            .next_row_into(&mut writer)
            .await
            .expect("second rowset row"));
        assert!(matches!(
            writer.take_row().as_slice(),
            [ColumnValues::Int(20)]
        ));
        assert!(!client.next_result().await.expect("drain to EOF"));
        assert!(!client.has_pending_results());
        assert!(!client.is_connection_dead());
    }
}

fn sql_error() -> Vec<u8> {
    let message = "fixture conversion error";
    let mut payload = 50000u32.to_le_bytes().to_vec();
    payload.extend([1, 16]);
    payload.extend(
        u16::try_from(message.len())
            .expect("short message")
            .to_le_bytes(),
    );
    payload.extend(message.encode_utf16().flat_map(u16::to_le_bytes));
    payload.extend([0, 0]); // server/procedure names
    payload.extend(1u32.to_le_bytes());
    let mut token = vec![0xaa];
    token.extend(
        u16::try_from(payload.len())
            .expect("short token")
            .to_le_bytes(),
    );
    token.extend(payload);
    token
}

#[tokio::test]
async fn first_row_cannot_hide_trailing_sql_error_and_connection_is_reusable() {
    let (mut client, mut peer) = connect().await;
    for _ in 0..2 {
        let mut body = METADATA.to_vec();
        body.extend([0xd1, 42, 0, 0, 0]);
        body.extend(DONE_MORE);
        body.extend(sql_error());
        body.extend(DONE);
        let server = async {
            request(&mut peer, 1).await;
            reply(&mut peer, &body, true).await;
        };
        let (first, ()) = timeout(DEADLINE, async {
            tokio::join!(client.query_first("first then error", &[]), server)
        })
        .await
        .expect("scalar deadline");
        assert!(matches!(
            first,
            Err(Error::Tds(
                mssql_tiberius_bridge::writer::TdsError::SqlServerError { .. }
            ))
        ));
        assert!(!client.is_connection_dead());
        assert!(!client.has_pending_results());
    }
    let mut body = METADATA.to_vec();
    body.extend([0xd1, 43, 0, 0, 0]);
    body.extend(DONE);
    let server = async {
        request(&mut peer, 1).await;
        reply(&mut peer, &body, true).await;
    };
    let (first, ()) = timeout(DEADLINE, async {
        tokio::join!(client.query_first("healthy", &[]), server)
    })
    .await
    .expect("healthy scalar deadline");
    assert_eq!(
        first.expect("first").expect("row").get::<i32, _>(0),
        Some(43)
    );
}

#[tokio::test]
async fn cancelling_mid_row_leaves_partial_writer_unpublished_and_client_dead() {
    let (mut client, mut peer) = connect().await;
    let mut metadata = METADATA.to_vec();
    *metadata.get_mut(1).expect("column count byte") = 2;
    metadata.extend(METADATA.get(3..).expect("column descriptor"));
    start(&mut client, &mut peer, &metadata).await;
    reply(&mut peer, &[0xd1, 42, 0, 0, 0, 43, 0], false).await;
    let mut writer = DefaultRowWriter::new(2);
    cancel_pending(client.next_row_into(&mut writer)).await;
    assert!(
        matches!(writer.take_row().as_slice(), [ColumnValues::Int(42)]),
        "fixture must decode one column before blocking on the second"
    );
    assert_dead(&mut client).await;
    drop(peer);
}

#[tokio::test]
async fn buffered_and_streamed_apis_still_work_after_incremental_close() {
    let (mut client, mut peer) = connect().await;
    start(&mut client, &mut peer, METADATA).await;
    reply(&mut peer, DONE, true).await;
    client.close_query().await.expect("incremental close");
    for streamed in [false, true] {
        let mut body = METADATA.to_vec();
        body.extend([0xd1, 7, 0, 0, 0]);
        body.extend(DONE_MORE);
        body.extend(METADATA);
        body.extend([0xd1, 8, 0, 0, 0]);
        body.extend(DONE);
        let server = async {
            request(&mut peer, 1).await;
            reply(&mut peer, &body, true).await;
        };
        let query = async {
            if streamed {
                client
                    .simple_query_streamed("streamed")
                    .try_collect::<Vec<_>>()
                    .await
            } else {
                Ok(client
                    .simple_query("buffered")
                    .await?
                    .into_results()
                    .into_iter()
                    .flatten()
                    .collect())
            }
        };
        let (rows, ()) = timeout(DEADLINE, async { tokio::join!(Box::pin(query), server) })
            .await
            .expect("existing API deadline");
        assert_eq!(
            rows.expect("rows")
                .iter()
                .map(|row| row.get::<i32, _>(0))
                .collect::<Vec<_>>(),
            [Some(7), Some(8)]
        );
        assert!(!client.has_pending_results());
        assert!(!client.is_connection_dead());
    }
}
