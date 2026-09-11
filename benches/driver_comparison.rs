//! Compare equivalent consumption of Tiberius, bridge, and direct RowWriter results.
//! Requires BENCH_DB_PASSWORD; all fixtures are connection-local temporary tables.

mod support;

use std::borrow::Cow;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures_util::TryStreamExt;
use mssql_tds::connection::tds_client::ResultSet;
use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::datatypes::sql_json::SqlJson;
use mssql_tds::datatypes::sql_string::{EncodingType, SqlString};
use mssql_tds::datatypes::sql_vector::SqlVector;
use mssql_tiberius_bridge::{Client, Row};
use support::{
    reverse_methods, row_counts, unsupported_values, ConnectionSettings, Shape, TiberiusClient,
    SELECT,
};
use uuid::Uuid;

impl Shape {
    fn expected(self, rows: u32) -> Digest {
        let mut digest = Digest::default();
        for n in 1..=rows {
            digest.add(0, u64::from(n));
            digest.add(2, (f64::from(n) * 1.5).to_bits());
            match self {
                Self::Numeric => {
                    if n % 10 == 0 {
                        digest.null(1);
                    } else {
                        digest.add(1, u64::from(n) * 1_000_000);
                    }
                    digest.add(3, u64::from(n % 2));
                }
                Self::Mixed => {
                    if n % 10 == 0 {
                        digest.null(1);
                        digest.null(3);
                    } else {
                        digest.text(1, &format!("row_{n}_caf\u{e9}_\u{4e2d}\u{6587}"));
                        digest.bytes(3, format!("payload_{n}").as_bytes());
                    }
                }
            }
            digest.rows += 1;
        }
        digest
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Digest {
    rows: u64,
    checksum: u64,
}

impl Digest {
    fn add(&mut self, col: usize, value: u64) {
        self.checksum = self
            .checksum
            .wrapping_add(value.rotate_left((col * 13) as u32));
    }

    fn null(&mut self, col: usize) {
        self.add(col, u64::MAX);
    }

    fn text(&mut self, col: usize, text: &str) {
        self.characters(col, text.chars());
    }

    fn characters(&mut self, col: usize, chars: impl Iterator<Item = char>) {
        let hash = chars.fold(5381u64, |hash, ch| {
            hash.wrapping_mul(33).wrapping_add(u64::from(ch))
        });
        self.add(col, hash);
    }

    fn bytes(&mut self, col: usize, bytes: &[u8]) {
        let hash = bytes.iter().fold(5381u64, |hash, byte| {
            hash.wrapping_mul(33).wrapping_add(u64::from(*byte))
        });
        self.add(col, hash);
    }

    fn bridge_row(&mut self, shape: Shape, row: &Row) {
        self.add(0, row.get::<i32, _>(0usize).unwrap() as u64);
        self.add(2, row.get::<f64, _>(2usize).unwrap().to_bits());
        match shape {
            Shape::Numeric => {
                match row.get::<i64, _>(1usize) {
                    Some(value) => self.add(1, value as u64),
                    None => self.null(1),
                }
                self.add(3, u64::from(row.get::<bool, _>(3usize).unwrap()));
            }
            Shape::Mixed => {
                match row.get::<&str, _>(1usize) {
                    Some(value) => self.text(1, value),
                    None => self.null(1),
                }
                match row.get::<&[u8], _>(3usize) {
                    Some(value) => self.bytes(3, value),
                    None => self.null(3),
                }
            }
        }
        self.rows += 1;
    }

    fn tiberius_row(&mut self, shape: Shape, row: &tiberius::Row) {
        self.add(0, row.get::<i32, _>(0usize).unwrap() as u64);
        self.add(2, row.get::<f64, _>(2usize).unwrap().to_bits());
        match shape {
            Shape::Numeric => {
                match row.get::<i64, _>(1usize) {
                    Some(value) => self.add(1, value as u64),
                    None => self.null(1),
                }
                self.add(3, u64::from(row.get::<bool, _>(3usize).unwrap()));
            }
            Shape::Mixed => {
                match row.get::<&str, _>(1usize) {
                    Some(value) => self.text(1, value),
                    None => self.null(1),
                }
                match row.get::<&[u8], _>(3usize) {
                    Some(value) => self.bytes(3, value),
                    None => self.null(3),
                }
            }
        }
        self.rows += 1;
    }
}

impl RowWriter for Digest {
    fn write_null(&mut self, col: usize) {
        self.null(col);
    }

    fn write_bool(&mut self, col: usize, value: bool) {
        self.add(col, u64::from(value));
    }

    fn write_i32(&mut self, col: usize, value: i32) {
        self.add(col, value as u64);
    }

    fn write_i64(&mut self, col: usize, value: i64) {
        self.add(col, value as u64);
    }

    fn write_f64(&mut self, col: usize, value: f64) {
        self.add(col, value.to_bits());
    }

    fn write_string(&mut self, col: usize, value: Cow<'_, [u8]>, encoding: EncodingType) {
        self.text(col, &SqlString::decode(&value, encoding));
    }

    fn write_bytes(&mut self, col: usize, value: Cow<'_, [u8]>) {
        self.bytes(col, &value);
    }

    unsupported_values! {
        write_u8(u8),
        write_i16(i16),
        write_f32(f32),
        write_decimal(DecimalParts),
        write_numeric(DecimalParts),
        write_date(SqlDate),
        write_time(SqlTime),
        write_datetime(SqlDateTime),
        write_smalldatetime(SqlSmallDateTime),
        write_datetime2(SqlDateTime2),
        write_datetimeoffset(SqlDateTimeOffset),
        write_money(SqlMoney),
        write_smallmoney(SqlSmallMoney),
        write_uuid(Uuid),
        write_xml(SqlXml),
        write_json(SqlJson),
        write_vector(SqlVector),
    }

    fn end_row(&mut self) {
        self.rows += 1;
    }
}

async fn bridge_stream(client: &mut Client, shape: Shape) -> Digest {
    let mut digest = Digest::default();
    let mut stream = client.simple_query_streamed(SELECT);
    while let Some(row) = stream.try_next().await.expect("bridge streamed row") {
        digest.bridge_row(shape, &row);
    }
    digest
}

async fn bridge_buffered(client: &mut Client, shape: Shape) -> Digest {
    let mut digest = Digest::default();
    let rows = client
        .simple_query(SELECT)
        .await
        .expect("bridge buffered query")
        .into_first_result();
    for row in rows {
        digest.bridge_row(shape, &row);
    }
    digest
}

async fn tiberius_stream(client: &mut TiberiusClient, shape: Shape) -> Digest {
    let mut digest = Digest::default();
    let mut stream = client
        .simple_query(SELECT)
        .await
        .expect("Tiberius query")
        .into_row_stream();
    while let Some(row) = stream.try_next().await.expect("Tiberius row") {
        digest.tiberius_row(shape, &row);
    }
    digest
}

async fn direct_writer(client: &mut Client) -> Digest {
    let mut digest = Digest::default();
    let inner = client.inner_mut();
    inner.close_query().await.expect("close prior query");
    inner
        .execute(SELECT.into(), ())
        .await
        .expect("direct writer query");
    while inner.on_rows() || inner.advance_to_rows().await.expect("advance to rows") {
        assert_eq!(inner.get_metadata().len(), 4);
        while inner
            .next_row_into(&mut digest)
            .await
            .expect("direct writer row")
        {}
        if !inner.advance_to_rows().await.expect("finish result set") {
            break;
        }
    }
    digest
}

fn driver_comparison(c: &mut Criterion) {
    let settings = ConnectionSettings::from_env();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime");
    let counts = row_counts();
    let mut methods = [
        "tiberius_stream",
        "bridge_stream",
        "bridge_buffered",
        "direct_writer",
    ];
    reverse_methods(&mut methods);

    for shape in [Shape::Numeric, Shape::Mixed] {
        let mut group = c.benchmark_group(shape.name());
        group.sample_size(20);
        group.warm_up_time(Duration::from_secs(2));
        group.measurement_time(Duration::from_secs(5));
        for &rows in &counts {
            let expected = shape.expected(rows);
            let setup = shape.setup_sql(rows);
            let mut bridge = rt.block_on(settings.bridge());
            let mut tiberius = rt.block_on(settings.tiberius());
            rt.block_on(async {
                bridge.simple_query(&setup).await.expect("bridge fixture");
                tiberius
                    .simple_query(&setup)
                    .await
                    .expect("Tiberius fixture")
                    .into_results()
                    .await
                    .expect("Tiberius fixture completion");
                assert_eq!(bridge_stream(&mut bridge, shape).await, expected);
                assert_eq!(bridge_buffered(&mut bridge, shape).await, expected);
                assert_eq!(tiberius_stream(&mut tiberius, shape).await, expected);
                assert_eq!(direct_writer(&mut bridge).await, expected);
            });
            group.throughput(Throughput::Elements(u64::from(rows)));
            for method in methods {
                group.bench_with_input(BenchmarkId::new(method, rows), &rows, |b, _| {
                    b.iter_custom(|iterations| {
                        rt.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let start = Instant::now();
                                let actual = match method {
                                    "tiberius_stream" => {
                                        tiberius_stream(&mut tiberius, shape).await
                                    }
                                    "bridge_stream" => bridge_stream(&mut bridge, shape).await,
                                    "bridge_buffered" => bridge_buffered(&mut bridge, shape).await,
                                    "direct_writer" => direct_writer(&mut bridge).await,
                                    _ => unreachable!(),
                                };
                                elapsed += start.elapsed();
                                assert_eq!(actual, expected, "{method} returned different values");
                            }
                            elapsed
                        })
                    });
                });
            }
        }
        group.finish();
    }
}

criterion_group!(benches, driver_comparison);
criterion_main!(benches);
