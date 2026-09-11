//! Compare row-to-Arrow adapters, direct prototypes, and the public Arrow read API.

#[path = "support/allocations.rs"]
mod allocations;
mod support;

use std::borrow::Cow;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
    LargeStringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use encoding_rs::{CoderResult, UTF_16LE};
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
use mssql_tiberius_bridge::{ArrowOptions, Client};
use support::{
    reverse_methods, row_counts, unsupported_values, ConnectionSettings, Shape, TiberiusClient,
    SELECT,
};
use uuid::Uuid;

#[global_allocator]
#[cfg(feature = "_bench_alloc")]
static ALLOCATOR: allocations::CountingAllocator = allocations::CountingAllocator;

const METHODS: [&str; 5] = [
    "tiberius_arrow",
    "bridge_arrow",
    "direct_arrow",
    "direct_arrow_reuse",
    "library_arrow",
];

#[derive(Clone)]
struct Dataset {
    shape: Shape,
    repeats: usize,
    schema: Arc<Schema>,
    sql: String,
}

impl Dataset {
    fn new(shape: Shape, repeats: usize) -> Self {
        let second = match shape {
            Shape::Numeric => DataType::Int64,
            Shape::Mixed => DataType::LargeUtf8,
        };
        let fourth = match shape {
            Shape::Numeric => DataType::Boolean,
            Shape::Mixed => DataType::LargeBinary,
        };
        let mut fields = Vec::new();
        let mut projection = Vec::new();
        for repeat in 0..repeats {
            for (name, data_type, nullable) in [
                ("id", DataType::Int32, false),
                ("c2", second.clone(), true),
                ("value", DataType::Float64, false),
                ("c4", fourth.clone(), matches!(shape, Shape::Mixed)),
            ] {
                let alias = format!("{name}_{repeat}");
                fields.push(Field::new(&alias, data_type, nullable));
                projection.push(format!("{name} AS {alias}"));
            }
        }
        let sql = format!(
            "SELECT {} FROM #throughput_rows ORDER BY id",
            projection.join(", ")
        );
        Self {
            shape,
            repeats,
            schema: Arc::new(Schema::new(fields)),
            sql,
        }
    }

    fn expected_batch(&self, start: usize, count: usize) -> RecordBatch {
        let mut writer = ArrowWriter::new(Arc::clone(&self.schema), count.max(1));
        for id in start..start + count {
            for repeat in 0..self.repeats {
                let col = repeat * 4;
                writer.write_i32(col, id as i32);
                writer.write_f64(col + 2, (id as f64) * 1.5);
                match self.shape {
                    Shape::Numeric => {
                        if id % 10 == 0 {
                            writer.write_null(col + 1);
                        } else {
                            writer.write_i64(col + 1, (id as i64) * 1_000_000);
                        }
                        writer.write_bool(col + 3, id % 2 != 0);
                    }
                    Shape::Mixed => {
                        if id % 10 == 0 {
                            writer.write_null(col + 1);
                            writer.write_null(col + 3);
                        } else {
                            let text = format!("row_{id}_caf\u{e9}_\u{4e2d}\u{6587}");
                            writer.append_text(col + 1, &text);
                            let bytes = format!("payload_{id}");
                            writer.write_bytes(col + 3, Cow::Borrowed(bytes.as_bytes()));
                        }
                    }
                }
            }
            writer.end_row();
        }
        writer.take_batch()
    }
}

enum ColumnBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    String(LargeStringBuilder),
    Binary(LargeBinaryBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, rows: usize) -> Self {
        match data_type {
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(rows)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(rows)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(rows)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(rows)),
            DataType::LargeUtf8 => Self::String(LargeStringBuilder::with_capacity(rows, rows * 64)),
            DataType::LargeBinary => {
                Self::Binary(LargeBinaryBuilder::with_capacity(rows, rows * 16))
            }
            _ => panic!("unsupported benchmark Arrow type: {data_type:?}"),
        }
    }

    fn null(&mut self) {
        match self {
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Boolean(builder) => builder.append_null(),
            Self::String(builder) => builder.append_null(),
            Self::Binary(builder) => builder.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
        }
    }
}

struct ArrowWriter {
    schema: Arc<Schema>,
    columns: Vec<ColumnBuilder>,
    rows: usize,
    batch_rows: usize,
    reuse_strings: bool,
    scratch: String,
}

impl ArrowWriter {
    fn new(schema: Arc<Schema>, batch_rows: usize) -> Self {
        let columns = Self::builders(&schema, batch_rows);
        Self {
            schema,
            columns,
            rows: 0,
            batch_rows,
            reuse_strings: false,
            scratch: String::new(),
        }
    }

    fn builders(schema: &Schema, batch_rows: usize) -> Vec<ColumnBuilder> {
        schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(field.data_type(), batch_rows))
            .collect()
    }

    fn take_batch(&mut self) -> RecordBatch {
        let arrays = self.columns.iter_mut().map(ColumnBuilder::finish).collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .expect("consistent Arrow column lengths and types");
        assert_eq!(batch.num_rows(), self.rows);
        self.rows = 0;
        self.columns = Self::builders(&self.schema, self.batch_rows);
        batch
    }

    fn append_text(&mut self, col: usize, text: &str) {
        let ColumnBuilder::String(builder) = &mut self.columns[col] else {
            panic!("string value against non-string column");
        };
        builder.append_value(text);
    }

    fn append_row(&mut self, row: &impl RowCells) {
        for (col, builder) in self.columns.iter_mut().enumerate() {
            match builder {
                ColumnBuilder::Int32(builder) => builder.append_option(row.int32(col)),
                ColumnBuilder::Int64(builder) => builder.append_option(row.int64(col)),
                ColumnBuilder::Float64(builder) => builder.append_option(row.float64(col)),
                ColumnBuilder::Boolean(builder) => builder.append_option(row.boolean(col)),
                ColumnBuilder::String(builder) => builder.append_option(row.text(col)),
                ColumnBuilder::Binary(builder) => builder.append_option(row.bytes(col)),
            }
        }
        self.end_row();
    }
}

trait RowCells {
    fn int32(&self, col: usize) -> Option<i32>;
    fn int64(&self, col: usize) -> Option<i64>;
    fn float64(&self, col: usize) -> Option<f64>;
    fn boolean(&self, col: usize) -> Option<bool>;
    fn text(&self, col: usize) -> Option<&str>;
    fn bytes(&self, col: usize) -> Option<&[u8]>;
}

macro_rules! row_cells {
    ($row:ty) => {
        impl RowCells for $row {
            fn int32(&self, col: usize) -> Option<i32> {
                self.get(col)
            }
            fn int64(&self, col: usize) -> Option<i64> {
                self.get(col)
            }
            fn float64(&self, col: usize) -> Option<f64> {
                self.get(col)
            }
            fn boolean(&self, col: usize) -> Option<bool> {
                self.get(col)
            }
            fn text(&self, col: usize) -> Option<&str> {
                self.get(col)
            }
            fn bytes(&self, col: usize) -> Option<&[u8]> {
                self.get(col)
            }
        }
    };
}

row_cells!(mssql_tiberius_bridge::Row);
row_cells!(tiberius::Row);

impl RowWriter for ArrowWriter {
    fn write_null(&mut self, col: usize) {
        self.columns[col].null();
    }

    fn write_bool(&mut self, col: usize, value: bool) {
        let ColumnBuilder::Boolean(builder) = &mut self.columns[col] else {
            panic!("bool value against non-bool column");
        };
        builder.append_value(value);
    }

    fn write_i32(&mut self, col: usize, value: i32) {
        let ColumnBuilder::Int32(builder) = &mut self.columns[col] else {
            panic!("i32 value against non-i32 column");
        };
        builder.append_value(value);
    }

    fn write_i64(&mut self, col: usize, value: i64) {
        let ColumnBuilder::Int64(builder) = &mut self.columns[col] else {
            panic!("i64 value against non-i64 column");
        };
        builder.append_value(value);
    }

    fn write_f64(&mut self, col: usize, value: f64) {
        let ColumnBuilder::Float64(builder) = &mut self.columns[col] else {
            panic!("f64 value against non-f64 column");
        };
        builder.append_value(value);
    }

    fn write_string(&mut self, col: usize, value: Cow<'_, [u8]>, encoding: EncodingType) {
        if !self.reuse_strings {
            self.append_text(col, &SqlString::decode(&value, encoding));
            return;
        }
        let ColumnBuilder::String(builder) = &mut self.columns[col] else {
            panic!("string value against non-string column");
        };
        match encoding {
            EncodingType::Utf16 => {
                self.scratch.clear();
                self.scratch
                    .reserve(value.len().checked_mul(3).expect("string size overflow"));
                // Match SqlString's replacement decoding and BOM handling, without
                // allocating a new UTF-8 string for every cell.
                let (result, read, _) =
                    UTF_16LE
                        .new_decoder()
                        .decode_to_string(&value, &mut self.scratch, true);
                assert_eq!(result, CoderResult::InputEmpty);
                assert_eq!(read, value.len());
                builder.append_value(&self.scratch);
            }
            EncodingType::Utf8 => {
                builder.append_value(std::str::from_utf8(&value).expect("valid UTF-8"));
            }
            _ => panic!("unexpected benchmark string encoding"),
        }
    }

    fn write_bytes(&mut self, col: usize, value: Cow<'_, [u8]>) {
        let ColumnBuilder::Binary(builder) = &mut self.columns[col] else {
            panic!("binary value against non-binary column");
        };
        builder.append_value(&value);
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
        assert!(self.rows <= self.batch_rows, "batch was not consumed");
    }
}

#[derive(Default)]
struct Consumer {
    rows: usize,
    batches: usize,
    validate: bool,
}

impl Consumer {
    fn accept(&mut self, batch: RecordBatch, dataset: &Dataset) {
        if self.validate {
            let expected = dataset.expected_batch(self.rows + 1, batch.num_rows());
            assert_eq!(batch.num_columns(), expected.num_columns());
            for (actual, expected) in batch
                .schema()
                .fields()
                .iter()
                .zip(expected.schema().fields())
            {
                assert_eq!(actual.name(), expected.name());
                assert_eq!(actual.data_type(), expected.data_type());
                assert_eq!(actual.is_nullable(), expected.is_nullable());
            }
            // The public API additionally preserves SQL-specific field metadata.
            assert_eq!(
                batch.columns(),
                expected.columns(),
                "Arrow cell values differ"
            );
        }
        self.rows += batch.num_rows();
        self.batches += 1;
        black_box(batch);
    }

    fn flush(&mut self, writer: &mut ArrowWriter, dataset: &Dataset) {
        if writer.rows > 0 {
            self.accept(writer.take_batch(), dataset);
        }
    }
}

async fn bridge_arrow(
    client: &mut Client,
    dataset: &Dataset,
    batch_rows: usize,
    validate: bool,
) -> Consumer {
    let mut writer = ArrowWriter::new(Arc::clone(&dataset.schema), batch_rows);
    let mut consumer = Consumer {
        validate,
        ..Consumer::default()
    };
    let mut stream = client.simple_query_streamed(&dataset.sql);
    while let Some(row) = stream.try_next().await.expect("bridge Arrow row") {
        writer.append_row(&row);
        if writer.rows == batch_rows {
            consumer.flush(&mut writer, dataset);
        }
    }
    consumer.flush(&mut writer, dataset);
    consumer
}

async fn tiberius_arrow(
    client: &mut TiberiusClient,
    dataset: &Dataset,
    batch_rows: usize,
    validate: bool,
) -> Consumer {
    let mut writer = ArrowWriter::new(Arc::clone(&dataset.schema), batch_rows);
    let mut consumer = Consumer {
        validate,
        ..Consumer::default()
    };
    let mut stream = client
        .simple_query(&dataset.sql)
        .await
        .expect("Tiberius Arrow query")
        .into_row_stream();
    while let Some(row) = stream.try_next().await.expect("Tiberius Arrow row") {
        writer.append_row(&row);
        if writer.rows == batch_rows {
            consumer.flush(&mut writer, dataset);
        }
    }
    consumer.flush(&mut writer, dataset);
    consumer
}

async fn direct_arrow(
    client: &mut Client,
    dataset: &Dataset,
    batch_rows: usize,
    validate: bool,
    reuse_strings: bool,
) -> Consumer {
    let mut writer = ArrowWriter::new(Arc::clone(&dataset.schema), batch_rows);
    writer.reuse_strings = reuse_strings;
    let mut consumer = Consumer {
        validate,
        ..Consumer::default()
    };
    let inner = client.inner_mut();
    inner.close_query().await.expect("close prior query");
    inner
        .execute(dataset.sql.clone(), ())
        .await
        .expect("direct Arrow query");
    while inner.on_rows() || inner.advance_to_rows().await.expect("advance to rows") {
        assert_eq!(inner.get_metadata().len(), dataset.schema.fields().len());
        while inner
            .next_row_into(&mut writer)
            .await
            .expect("direct Arrow row")
        {
            if writer.rows == batch_rows {
                consumer.flush(&mut writer, dataset);
            }
        }
        consumer.flush(&mut writer, dataset);
        if !inner.advance_to_rows().await.expect("finish result set") {
            break;
        }
    }
    consumer
}

async fn library_arrow(
    client: &mut Client,
    dataset: &Dataset,
    batch_rows: usize,
    validate: bool,
) -> Consumer {
    let options = ArrowOptions {
        batch_size: batch_rows,
        // Keep identical row boundaries even for the widest batch-size experiments.
        batch_bytes: 128 * 1024 * 1024,
        ..ArrowOptions::default()
    };
    let mut consumer = Consumer {
        validate,
        ..Consumer::default()
    };
    let mut stream = client.simple_query_arrow_with_options(&dataset.sql, options);
    while let Some(result) = stream.try_next().await.expect("public Arrow read") {
        assert_eq!(result.result_index, 0);
        consumer.accept(result.batch, dataset);
    }
    consumer
}

fn positive_list(name: &str, default: &str, max: usize) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.into())
        .split(',')
        .map(|value| {
            let value = value
                .parse()
                .expect("expected comma-separated positive integers");
            assert!((1..=max).contains(&value), "{name} must be in 1..={max}");
            value
        })
        .collect()
}

fn allocation_mode() -> bool {
    match std::env::var("BENCH_ARROW_ALLOCATIONS").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(std::env::VarError::NotPresent) => false,
        _ => panic!("BENCH_ARROW_ALLOCATIONS must be 0 or 1"),
    }
}

async fn run_method(
    method: &str,
    bridge: &mut Client,
    tiberius: &mut TiberiusClient,
    dataset: &Dataset,
    batch_rows: usize,
    validate: bool,
) -> Consumer {
    match method {
        "tiberius_arrow" => tiberius_arrow(tiberius, dataset, batch_rows, validate).await,
        "bridge_arrow" => bridge_arrow(bridge, dataset, batch_rows, validate).await,
        "direct_arrow" => direct_arrow(bridge, dataset, batch_rows, validate, false).await,
        "direct_arrow_reuse" => direct_arrow(bridge, dataset, batch_rows, validate, true).await,
        "library_arrow" => library_arrow(bridge, dataset, batch_rows, validate).await,
        _ => unreachable!(),
    }
}

async fn validate_edges(settings: &ConnectionSettings) {
    for shape in [Shape::Numeric, Shape::Mixed] {
        let mut bridge = settings.bridge().await;
        let mut tiberius = settings.tiberius().await;
        let setup = shape.setup_sql(33);
        bridge
            .simple_query(&setup)
            .await
            .expect("edge-case fixture");
        tiberius
            .simple_query(&setup)
            .await
            .expect("edge-case fixture")
            .into_results()
            .await
            .expect("edge-case fixture completion");
        for repeats in [1, 4] {
            let dataset = Dataset::new(shape, repeats);
            for batch_rows in [1, 8, 33, 64] {
                for method in METHODS {
                    let result = run_method(
                        method,
                        &mut bridge,
                        &mut tiberius,
                        &dataset,
                        batch_rows,
                        true,
                    )
                    .await;
                    assert_eq!(result.rows, 33);
                    assert_eq!(result.batches, 33usize.div_ceil(batch_rows));
                }
            }
            let mut empty = dataset.clone();
            empty.sql = empty.sql.replace("ORDER BY id", "WHERE 1 = 0 ORDER BY id");
            for method in METHODS {
                let result = run_method(method, &mut bridge, &mut tiberius, &empty, 8, true).await;
                assert_eq!(
                    (result.rows, result.batches),
                    (0, usize::from(method == "library_arrow"))
                );
            }
        }
        bridge
            .simple_query(SELECT)
            .await
            .expect("connection remains reusable");
    }
}

fn validate_string_decoding() {
    let dataset = Dataset::new(Shape::Mixed, 1);
    let mut baseline = ArrowWriter::new(Arc::clone(&dataset.schema), 1);
    let mut reused = ArrowWriter::new(Arc::clone(&dataset.schema), 1);
    reused.reuse_strings = true;
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    let utf16 = [
        vec![],
        vec![0xff, 0xfe, 0x61, 0],
        vec![0xfe, 0xff, 0, 0x61],
        vec![0, 0xd8],
        vec![0, 0xdc],
        vec![0x61],
        vec![0x61, 0, 0, 0xd8, 0x62, 0],
        "\u{1f600}"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect(),
    ];
    for bytes in &utf16 {
        for owned in [false, true] {
            for writer in [&mut baseline, &mut reused] {
                writer.write_i32(0, 1);
                let value = if owned {
                    Cow::Owned(bytes.clone())
                } else {
                    Cow::Borrowed(bytes.as_slice())
                };
                writer.write_string(1, value, EncodingType::Utf16);
                writer.write_f64(2, 1.5);
                writer.write_bytes(3, Cow::Borrowed(b"binary"));
                writer.end_row();
            }
            expected.push(baseline.take_batch());
            actual.push(reused.take_batch());
        }
    }
    for text in ["", "ASCII", "\u{e9}\u{4e2d}\u{1f600}"] {
        for writer in [&mut baseline, &mut reused] {
            writer.write_i32(0, 1);
            writer.write_string(1, Cow::Borrowed(text.as_bytes()), EncodingType::Utf8);
            writer.write_f64(2, 1.5);
            writer.write_null(3);
            writer.end_row();
        }
        expected.push(baseline.take_batch());
        actual.push(reused.take_batch());
    }
    assert_eq!(
        actual, expected,
        "reused decoding or retained batches changed"
    );
}

fn arrow_comparison(c: &mut Criterion) {
    validate_string_decoding();
    let settings = ConnectionSettings::from_env();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime");
    let counts = row_counts();
    let repeats = positive_list("BENCH_ARROW_REPEATS", "1", 8);
    let batches = positive_list("BENCH_ARROW_BATCH_ROWS", "8192", 65536);
    let allocations = allocation_mode();
    assert!(
        !allocations || cfg!(feature = "_bench_alloc"),
        "allocation measurements require --features _bench_alloc,arrow"
    );
    assert!(
        allocations || !cfg!(feature = "_bench_alloc"),
        "set BENCH_ARROW_ALLOCATIONS=1; use _bench,arrow without _bench_alloc for timing"
    );
    let mut methods = METHODS;
    reverse_methods(&mut methods);
    rt.block_on(validate_edges(&settings));

    for shape in [Shape::Numeric, Shape::Mixed] {
        for &width in &repeats {
            let dataset = Dataset::new(shape, width);
            for &rows in &counts {
                let mut bridge = rt.block_on(settings.bridge());
                let mut tiberius = rt.block_on(settings.tiberius());
                let setup = format!(
                    "{} CREATE UNIQUE CLUSTERED INDEX ix_rows ON #throughput_rows(id);",
                    shape.setup_sql(rows)
                );
                rt.block_on(async {
                    bridge.simple_query(&setup).await.expect("Arrow fixture");
                    tiberius
                        .simple_query(&setup)
                        .await
                        .expect("Arrow fixture")
                        .into_results()
                        .await
                        .expect("Arrow fixture completion");
                });
                for &batch_rows in &batches {
                    let name = format!(
                        "arrow_{}_{}cols_batch{}",
                        shape.name(),
                        width * 4,
                        batch_rows
                    );
                    let mut group = c.benchmark_group(&name);
                    group.sample_size(20);
                    group.warm_up_time(Duration::from_secs(2));
                    group.measurement_time(Duration::from_secs(5));
                    group.throughput(Throughput::Elements(u64::from(rows)));
                    for method in methods {
                        let preflight = rt.block_on(run_method(
                            method,
                            &mut bridge,
                            &mut tiberius,
                            &dataset,
                            batch_rows,
                            true,
                        ));
                        assert_eq!(preflight.rows, rows as usize);
                        assert_eq!(preflight.batches, (rows as usize).div_ceil(batch_rows));
                        if allocations {
                            let guard = allocations::Measurement::start();
                            let result = rt.block_on(run_method(
                                method,
                                &mut bridge,
                                &mut tiberius,
                                &dataset,
                                batch_rows,
                                false,
                            ));
                            let (calls, bytes) = guard.finish();
                            assert_eq!(result.rows, rows as usize);
                            println!("allocations,{name},{method},{rows},{calls},{bytes}");
                            continue;
                        }
                        group.bench_with_input(BenchmarkId::new(method, rows), &rows, |b, _| {
                            b.iter_custom(|iterations| {
                                rt.block_on(async {
                                    let mut elapsed = Duration::ZERO;
                                    for _ in 0..iterations {
                                        let start = Instant::now();
                                        let result = run_method(
                                            method,
                                            &mut bridge,
                                            &mut tiberius,
                                            &dataset,
                                            batch_rows,
                                            false,
                                        )
                                        .await;
                                        elapsed += start.elapsed();
                                        assert_eq!(result.rows, rows as usize);
                                        assert_eq!(
                                            result.batches,
                                            (rows as usize).div_ceil(batch_rows)
                                        );
                                    }
                                    elapsed
                                })
                            });
                        });
                    }
                    group.finish();
                }
            }
        }
    }
}

criterion_group!(benches, arrow_comparison);
criterion_main!(benches);
