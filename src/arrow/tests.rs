use super::*;
use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    StringArray, StructArray, Time64NanosecondArray, UInt32Array, UInt8Array,
};
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqldatatypes::TypeInfo;
use mssql_tds::test_client_support::{int_columns, udt_column_with_metadata};
use mssql_tds::token::tokens::SqlCollation;

fn metadata(info: TypeInfo) -> ColumnMetadata {
    let mut meta = int_columns(1).remove(0);
    meta.data_type = info.tds_type;
    meta.type_info = info;
    meta
}

fn fixed(data_type: TdsDataType) -> ColumnMetadata {
    metadata(TypeInfo::fixed_len(data_type).unwrap())
}

fn variable(data_type: TdsDataType, length: usize) -> ColumnMetadata {
    metadata(TypeInfo::var_len(data_type, length).unwrap())
}

fn scaled(data_type: TdsDataType, scale: u8) -> ColumnMetadata {
    let time_length = match scale {
        0..=2 => 3,
        3..=4 => 4,
        _ => 5,
    };
    let length = time_length
        + match data_type {
            TdsDataType::DateTime2N => 3,
            TdsDataType::DateTimeOffsetN => 5,
            _ => 0,
        };
    metadata(TypeInfo::var_len_scale(data_type, length, scale).unwrap())
}

fn decimal_meta(precision: u8, scale: u8, numeric: bool) -> ColumnMetadata {
    let data_type = if numeric {
        TdsDataType::NumericN
    } else {
        TdsDataType::DecimalN
    };
    let length = match precision {
        1..=9 => 5,
        10..=19 => 9,
        20..=28 => 13,
        _ => 17,
    };
    metadata(TypeInfo::var_len_precision_scale(data_type, length, precision, scale).unwrap())
}

fn text_meta() -> ColumnMetadata {
    metadata(TypeInfo::partial_len(TdsDataType::NVarChar, 65_535, None).unwrap())
}

fn vector_meta(dimensions: usize, base_type: u8) -> ColumnMetadata {
    let size = if base_type == 1 { 2 } else { 4 };
    metadata(
        TypeInfo::var_len_scale(
            TdsDataType::Vector,
            VECTOR_HEADER_SIZE + dimensions * size,
            base_type,
        )
        .unwrap(),
    )
}

fn writer(metadata: &[ColumnMetadata]) -> ArrowRowWriter {
    ArrowRowWriter::new(metadata, ArrowOptions::default()).unwrap()
}

fn array<T: Array + 'static>(batch: &RecordBatch, col: usize) -> &T {
    batch.column(col).as_any().downcast_ref::<T>().unwrap()
}

fn temporal_child<T: Array + 'static>(batch: &RecordBatch, col: usize, child: usize) -> &T {
    array::<StructArray>(batch, col)
        .column(child)
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}

fn utf16(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn assert_error(writer: &mut ArrowRowWriter, contains: &str) {
    let error = writer.check_error().unwrap_err().to_string();
    assert!(
        error.contains(contains),
        "{error:?} does not contain {contains:?}"
    );
    assert!(writer.take_batch().is_err());
}

#[test]
fn options_defaults_validation_and_bounded_initial_allocations() {
    let defaults = ArrowOptions::default();
    assert_eq!(defaults.batch_size, 8192);
    assert_eq!(defaults.batch_bytes, 8 * 1024 * 1024);
    assert_eq!(defaults.max_row_bytes, None);
    assert_eq!(defaults.max_value_bytes, None);
    defaults.validate().unwrap();
    for field in 0..4 {
        for invalid in [0, usize::MAX] {
            let mut options = defaults.clone();
            match field {
                0 => options.batch_size = invalid,
                1 => options.batch_bytes = invalid,
                2 => options.max_row_bytes = Some(invalid),
                _ => options.max_value_bytes = Some(invalid),
            }
            assert!(options.validate().is_err());
        }
    }
    let huge = ArrowOptions {
        batch_size: isize::MAX as usize,
        batch_bytes: isize::MAX as usize,
        max_row_bytes: Some(isize::MAX as usize),
        max_value_bytes: Some(isize::MAX as usize),
    };
    huge.validate().unwrap();
    assert_eq!(
        ArrowRowWriter::new(&[fixed(TdsDataType::Int4)], huge.clone())
            .unwrap()
            .initial_rows,
        8192
    );
    let wide = vec![vector_meta(1998, 0); 100];
    let w = ArrowRowWriter::new(&wide, huge).unwrap();
    assert!(w.initial_rows < 16);
    assert!(w.initial_rows * 100 * (1998 * 5 + 1) <= DEFAULT_BYTES);
    let tiny = ArrowOptions {
        batch_bytes: 1,
        ..defaults
    };
    let mut w = ArrowRowWriter::new(&[text_meta()], tiny).unwrap();
    assert_eq!(w.initial_rows, 0);
    w.write_string(0, Cow::Borrowed(b"allowed"), EncodingType::Utf8);
    w.end_row();
    w.check_error().unwrap();
    assert!(w.should_flush());
    assert_eq!(w.take_batch().unwrap().num_rows(), 1);
}

#[test]
fn strict_numeric_schema_and_nulls() {
    let metadata = [
        fixed(TdsDataType::Bit),
        variable(TdsDataType::IntN, 1),
        variable(TdsDataType::IntN, 2),
        variable(TdsDataType::IntN, 4),
        variable(TdsDataType::IntN, 8),
        variable(TdsDataType::FltN, 4),
        variable(TdsDataType::FltN, 8),
    ];
    let mut w = writer(&metadata);
    w.write_bool(0, true);
    w.write_u8(1, u8::MAX);
    w.write_i16(2, i16::MIN);
    w.write_i32(3, i32::MIN);
    w.write_i64(4, i64::MIN);
    w.write_f32(5, -1.5);
    w.write_f64(6, 2.25);
    w.end_row();
    let first_row_bytes = w.last_row_bytes();
    for col in 0..metadata.len() {
        w.write_null(col);
    }
    w.end_row();
    w.check_error().unwrap();
    assert_eq!(first_row_bytes, 35);
    assert_eq!(w.row_count(), 2);
    let batch = w.take_batch().unwrap();
    assert!(array::<BooleanArray>(&batch, 0).value(0));
    assert_eq!(array::<UInt8Array>(&batch, 1).value(0), 255);
    assert_eq!(array::<Int16Array>(&batch, 2).value(0), i16::MIN);
    assert_eq!(array::<Int32Array>(&batch, 3).value(0), i32::MIN);
    assert_eq!(array::<Int64Array>(&batch, 4).value(0), i64::MIN);
    assert_eq!(array::<Float32Array>(&batch, 5).value(0), -1.5);
    assert_eq!(array::<Float64Array>(&batch, 6).value(0), 2.25);
    for array in batch.columns() {
        assert!(array.is_null(1));
        assert_eq!(array.len(), 2);
    }
}

#[test]
fn multiple_owned_batches_empty_schema_and_scratch_reuse() {
    let options = ArrowOptions {
        batch_size: 2,
        ..ArrowOptions::default()
    };
    let mut w = ArrowRowWriter::new(&[text_meta()], options).unwrap();
    let payload = utf16(&"caf\u{e9} \u{4e2d} \u{1f984}".repeat(2000));
    let mut batches = Vec::new();
    for _ in 0..3 {
        for _ in 0..2 {
            w.write_string(0, Cow::Borrowed(&payload), EncodingType::Utf16);
            w.end_row();
        }
        w.check_error().unwrap();
        assert!(w.should_flush());
        let capacity = w.scratch.capacity();
        batches.push(w.take_batch().unwrap());
        assert_eq!(w.row_count(), 0);
        assert_eq!(w.last_row_bytes(), 0);
        assert!(!w.should_flush());
        assert_eq!(w.scratch.capacity(), capacity);
    }
    drop(w);
    for batch in &batches {
        assert_eq!(
            array::<LargeStringArray>(batch, 0).value(1),
            SqlString::decode(&payload, EncodingType::Utf16)
        );
    }
    let mut w = writer(&[fixed(TdsDataType::Int4), text_meta()]);
    let empty = w.take_batch().unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.num_columns(), 2);
    assert_eq!(empty.schema().field(1).data_type(), &DataType::LargeUtf8);
    let mut empty_schema = writer(&[]);
    assert_eq!(empty_schema.take_batch().unwrap().num_columns(), 0);
    empty_schema.end_row();
    assert_eq!(empty_schema.take_batch().unwrap().num_rows(), 1);
}

#[test]
fn string_borrowed_owned_bom_replacement_and_codepage_parity() {
    let collation = SqlCollation {
        info: 0x0409,
        lcid_language_id: 0x0409,
        col_flags: 0,
        sort_id: 0,
    };
    let unknown = SqlCollation {
        info: 0x0f_ffff,
        lcid_language_id: 0x0f_ffff,
        col_flags: 0,
        sort_id: 0,
    };
    let cases = [
        (EncodingType::Utf8, b"\xef\xbb\xbfhello".to_vec()),
        (EncodingType::Utf16, utf16("caf\u{e9} \u{4e2d} \u{1f600}")),
        (EncodingType::Utf16, vec![0xff, 0xfe, b'a', 0]),
        (EncodingType::Utf16, vec![0xfe, 0xff, 0, b'a']),
        (EncodingType::Utf16, vec![0x00, 0xd8, b'b', 0]),
        (EncodingType::Utf16, vec![0x61]),
        (EncodingType::Utf16, Vec::new()),
        (EncodingType::LcidBased(collation), vec![0x80, 0xe9]),
        (EncodingType::LcidBased(unknown), vec![0x80, 0xe9]),
    ];
    for (encoding, payload) in cases {
        let expected = SqlString::decode(&payload, encoding);
        let mut w = writer(&[text_meta()]);
        w.write_string(0, Cow::Borrowed(&payload), encoding);
        w.end_row();
        w.write_string(0, Cow::Owned(payload), encoding);
        w.end_row();
        w.check_error().unwrap();
        let batch = w.take_batch().unwrap();
        assert_eq!(array::<LargeStringArray>(&batch, 0).value(0), expected);
        assert_eq!(array::<LargeStringArray>(&batch, 0).value(1), expected);
    }
}

#[test]
fn invalid_utf8_and_unresolved_encodings_are_sticky_errors_not_panics() {
    for owned in [false, true] {
        let mut w = writer(&[text_meta()]);
        let value = if owned {
            Cow::Owned(vec![0xff])
        } else {
            Cow::Borrowed(&[0xff][..])
        };
        w.write_string(0, value, EncodingType::Utf8);
        w.end_row();
        assert_error(&mut w, "Invalid UTF-8");
        assert_eq!(w.rows, 0);
    }
    let mut w = writer(&[text_meta()]);
    w.write_string(0, Cow::Borrowed(b""), EncodingType::DelayedSet);
    assert_error(&mut w, "Unresolved");
}

#[test]
fn xml_json_uuid_binary_and_null_payloads() {
    let metadata = [
        metadata(TypeInfo::partial_len(TdsDataType::Xml, 65_535, None).unwrap()),
        metadata(TypeInfo::partial_len(TdsDataType::Json, 65_535, None).unwrap()),
        variable(TdsDataType::Guid, 16),
        variable(TdsDataType::BigVarBinary, 4096),
    ];
    let uuid = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
    let mut w = writer(&metadata);
    w.write_xml(
        0,
        SqlXml {
            bytes: vec![0xff, 0xfe, 0x61],
        },
    );
    w.write_json(
        1,
        SqlJson {
            bytes: br#"{"hello":"world"}"#.to_vec(),
        },
    );
    w.write_uuid(2, uuid);
    w.write_bytes(3, Cow::Borrowed(&[0, 255, 1]));
    w.end_row();
    for col in 0..4 {
        w.write_null(col);
    }
    w.end_row();
    w.check_error().unwrap();
    let batch = w.take_batch().unwrap();
    assert_eq!(array::<LargeStringArray>(&batch, 0).value(0), "\u{fffd}");
    assert_eq!(
        array::<LargeStringArray>(&batch, 1).value(0),
        r#"{"hello":"world"}"#
    );
    assert_eq!(
        array::<StringArray>(&batch, 2).value(0),
        uuid.hyphenated().to_string()
    );
    assert_eq!(array::<LargeBinaryArray>(&batch, 3).value(0), &[0, 255, 1]);
    for col in batch.columns() {
        assert!(col.is_null(1));
    }
    let mut w = writer(&metadata);
    w.write_xml(0, SqlXml { bytes: vec![] });
    w.write_json(1, SqlJson { bytes: vec![255] });
    assert_error(&mut w, "Invalid UTF-8");
}

#[test]
fn source_metadata_and_string_binary_type_families() {
    let collation = SqlCollation {
        info: 0x0409,
        lcid_language_id: 0x0409,
        col_flags: 2,
        sort_id: 52,
    };
    for (tds_type, sql_name) in [
        (TdsDataType::BigChar, "char"),
        (TdsDataType::BigVarChar, "varchar"),
        (TdsDataType::NChar, "nchar"),
        (TdsDataType::NVarChar, "nvarchar"),
        (TdsDataType::Text, "text"),
        (TdsDataType::NText, "ntext"),
    ] {
        let mut meta = metadata(TypeInfo::var_len_string(tds_type, 128, Some(collation)).unwrap());
        meta.column_name = "Named".into();
        meta.user_type = 17;
        meta.flags = 0;
        let (kind, field) = column_schema(&meta).unwrap();
        assert_eq!(kind, Kind::Text);
        assert_eq!(field.name(), "Named");
        assert!(!field.is_nullable());
        assert_eq!(field.data_type(), &DataType::LargeUtf8);
        assert_eq!(field.metadata()["mssql.type"], sql_name);
        assert_eq!(field.metadata()["mssql.user_type"], "17");
        assert_eq!(field.metadata()["mssql.length"], "128");
        assert_eq!(field.metadata()["mssql.collation.info"], "1033");
        assert_eq!(field.metadata()["mssql.collation.lcid"], "1033");
        assert_eq!(field.metadata()["mssql.collation.flags"], "2");
        assert_eq!(field.metadata()["mssql.collation.sort_id"], "52");
    }
    for tds_type in [
        TdsDataType::BigBinary,
        TdsDataType::BigVarBinary,
        TdsDataType::Image,
    ] {
        let (_, field) = column_schema(&variable(tds_type, 16)).unwrap();
        assert_eq!(field.data_type(), &DataType::LargeBinary);
    }
    for (tds_type, kind) in [
        (TdsDataType::VarChar, Kind::Text),
        (TdsDataType::Char, Kind::Text),
        (TdsDataType::VarBinary, Kind::Binary),
        (TdsDataType::Binary, Kind::Binary),
    ] {
        let mut meta = fixed(TdsDataType::Int4);
        meta.data_type = tds_type;
        meta.type_info.tds_type = tds_type;
        assert_eq!(column_schema(&meta).unwrap().0, kind);
    }
    for (schema, name, expected) in [
        ("sys", "geography", "geography"),
        ("SYS", "GEOMETRY", "geometry"),
        ("dbo", "geography", "udt"),
        ("dbo", "custom", "udt"),
    ] {
        let meta = udt_column_with_metadata(8000, "db", schema, name, "assembly");
        let (kind, field) = column_schema(&meta).unwrap();
        assert_eq!(kind, Kind::Binary);
        assert_eq!(field.metadata()["mssql.type"], expected);
        assert_eq!(field.metadata()["mssql.udt.database"], "db");
        assert_eq!(field.metadata()["mssql.udt.schema"], schema);
        assert_eq!(field.metadata()["mssql.udt.name"], name);
        assert_eq!(
            field.metadata()["mssql.udt.assembly_qualified_name"],
            "assembly"
        );
        assert_eq!(field.metadata()["mssql.udt.max_byte_size"], "8000");
        let mut w = writer(&[meta]);
        w.write_bytes(0, Cow::Owned(vec![1, 2, 3]));
        w.end_row();
        assert_eq!(
            array::<LargeBinaryArray>(&w.take_batch().unwrap(), 0).value(0),
            &[1, 2, 3]
        );
    }
}

#[test]
fn decimal_38_exact_sign_scale_and_money_boundaries() {
    for numeric in [false, true] {
        for scale in [0, 7, 38] {
            let mut w = writer(&[decimal_meta(38, scale, numeric)]);
            let magnitude = 10u128.pow(38) - 1;
            for positive in [true, false] {
                let value = DecimalParts::new(positive, 38, scale, magnitude);
                if numeric {
                    w.write_numeric(0, value);
                } else {
                    w.write_decimal(0, value);
                }
                w.end_row();
            }
            w.write_null(0);
            w.end_row();
            w.check_error().unwrap();
            let batch = w.take_batch().unwrap();
            let values = array::<Decimal128Array>(&batch, 0);
            assert_eq!(values.value(0), magnitude as i128);
            assert_eq!(values.value(1), -(magnitude as i128));
            assert!(values.is_null(2));
            assert_eq!(values.data_type(), &DataType::Decimal128(38, scale as i8));
            assert_eq!(batch.schema().field(0).metadata()["mssql.precision"], "38");
            assert_eq!(
                batch.schema().field(0).metadata()["mssql.scale"],
                scale.to_string()
            );
        }
    }
    let metadata = [fixed(TdsDataType::Money), fixed(TdsDataType::Money4)];
    let mut w = writer(&metadata);
    for (money, smallmoney) in [(i64::MIN, i32::MIN), (i64::MAX, i32::MAX), (-1, -1)] {
        w.write_money(
            0,
            SqlMoney {
                lsb_part: money as i32,
                msb_part: (money >> 32) as i32,
            },
        );
        w.write_smallmoney(
            1,
            SqlSmallMoney {
                int_val: smallmoney,
            },
        );
        w.end_row();
    }
    w.check_error().unwrap();
    let batch = w.take_batch().unwrap();
    assert_eq!(
        array::<Decimal128Array>(&batch, 0).values(),
        &[i64::MIN as i128, i64::MAX as i128, -1]
    );
    assert_eq!(
        array::<Decimal128Array>(&batch, 1).values(),
        &[i32::MIN as i128, i32::MAX as i128, -1]
    );
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Decimal128(19, 4)
    );
    assert_eq!(
        batch.schema().field(1).data_type(),
        &DataType::Decimal128(10, 4)
    );
    assert_eq!(batch.schema().field(0).metadata()["mssql.scale"], "4");
}

#[test]
fn decimal_magnitude_precision_scale_and_callback_mismatches_rejected() {
    for value in [
        DecimalParts::new(true, 38, 0, 10u128.pow(38)),
        DecimalParts::new(false, 38, 0, u128::MAX),
        DecimalParts::new(true, 37, 0, 1),
        DecimalParts::new(true, 38, 1, 1),
        DecimalParts::new(true, 255, 0, 1),
        DecimalParts::new(true, 0, 0, 1),
    ] {
        let mut w = writer(&[decimal_meta(38, 0, false)]);
        w.write_decimal(0, value);
        assert!(w.check_error().is_err());
        assert!(w.take_batch().is_err());
    }
    let mut w = writer(&[decimal_meta(38, 0, true)]);
    w.write_decimal(0, DecimalParts::new(true, 38, 0, 1));
    assert_error(&mut w, "callback type");
}

#[test]
fn dates_all_time_scales_and_full_datetime2_range_are_lossless() {
    for scale in 0..=7 {
        let metadata = [
            variable(TdsDataType::DateN, 3),
            scaled(TdsDataType::TimeN, scale),
            scaled(TdsDataType::DateTime2N, scale),
        ];
        let mut w = writer(&metadata);
        let last_tick = TICKS_PER_DAY - 10u64.pow((7 - scale) as u32);
        for (days, ticks) in [(0, 0), (MAX_SQL_DAYS, last_tick)] {
            w.write_date(0, SqlDate::create(days).unwrap());
            w.write_time(
                1,
                SqlTime {
                    time_nanoseconds: ticks,
                    scale,
                },
            );
            w.write_datetime2(
                2,
                SqlDateTime2 {
                    days,
                    time: SqlTime {
                        time_nanoseconds: ticks,
                        scale,
                    },
                },
            );
            w.end_row();
        }
        for col in 0..3 {
            w.write_null(col);
        }
        w.end_row();
        w.check_error().unwrap();
        let batch = w.take_batch().unwrap();
        assert_eq!(array::<Date32Array>(&batch, 0).value(0), -719_162);
        assert_eq!(array::<Date32Array>(&batch, 0).value(1), 2_932_896);
        assert_eq!(
            array::<Time64NanosecondArray>(&batch, 1).value(1),
            (last_tick * 100) as i64
        );
        assert_eq!(
            temporal_child::<Date32Array>(&batch, 2, 0).value(1),
            2_932_896
        );
        assert_eq!(
            temporal_child::<Time64NanosecondArray>(&batch, 2, 1).value(1),
            (last_tick * 100) as i64
        );
        let structure = array::<StructArray>(&batch, 2);
        assert!(structure.is_null(2));
        for child in structure.columns() {
            assert_eq!(child.len(), 3);
            assert_eq!(child.null_count(), 0);
        }
    }
}

#[test]
fn datetime_exact_ticks_and_smalldatetime_minute_boundaries() {
    let metadata = [
        variable(TdsDataType::DateTimeN, 8),
        variable(TdsDataType::DateTimeN, 4),
    ];
    let mut w = writer(&metadata);
    for (days, ticks, small_days, minutes) in
        [(-53_690, 1, 0, 0), (2_958_463, 25_919_999, u16::MAX, 1439)]
    {
        w.write_datetime(0, SqlDateTime { days, time: ticks });
        w.write_smalldatetime(
            1,
            SqlSmallDateTime {
                days: small_days,
                time: minutes,
            },
        );
        w.end_row();
    }
    w.write_null(0);
    w.write_null(1);
    w.end_row();
    w.check_error().unwrap();
    let batch = w.take_batch().unwrap();
    assert_eq!(
        temporal_child::<UInt32Array>(&batch, 0, 1).values(),
        &[1, 25_919_999, 0]
    );
    assert_eq!(
        temporal_child::<Date32Array>(&batch, 0, 0).value(0),
        -79_257
    );
    assert_eq!(
        temporal_child::<Date32Array>(&batch, 0, 0).value(1),
        2_932_896
    );
    assert_eq!(temporal_child::<Date32Array>(&batch, 1, 0).value(1), 39_968);
    assert_eq!(
        temporal_child::<Time64NanosecondArray>(&batch, 1, 1).value(1),
        86_340_000_000_000
    );
    assert!(array::<StructArray>(&batch, 0).is_null(2));
    assert!(array::<StructArray>(&batch, 1).is_null(2));
}

#[test]
fn datetimeoffset_preserves_utc_wire_clock_and_offsets() {
    let mut w = writer(&[scaled(TdsDataType::DateTimeOffsetN, 7)]);
    for (days, time, offset) in [
        (0, 0, 0),
        (MAX_SQL_DAYS, TICKS_PER_DAY - 1, 0),
        (1000, 1, -840),
        (1000, TICKS_PER_DAY - 1, 840),
    ] {
        w.write_datetimeoffset(
            0,
            SqlDateTimeOffset {
                datetime2: SqlDateTime2 {
                    days,
                    time: SqlTime {
                        time_nanoseconds: time,
                        scale: 7,
                    },
                },
                offset,
            },
        );
        w.end_row();
    }
    w.write_null(0);
    w.end_row();
    w.check_error().unwrap();
    let batch = w.take_batch().unwrap();
    assert_eq!(
        batch.schema().field(0).metadata()["mssql.time_basis"],
        "UTC"
    );
    assert_eq!(
        temporal_child::<Date32Array>(&batch, 0, 0).value(2),
        1000 - SQL_EPOCH_DAYS
    );
    assert_eq!(
        temporal_child::<Time64NanosecondArray>(&batch, 0, 1).value(2),
        100
    );
    assert_eq!(
        temporal_child::<Int16Array>(&batch, 0, 2).values(),
        &[0, 0, -840, 840, 0]
    );
    assert!(array::<StructArray>(&batch, 0).is_null(4));
    assert_eq!(temporal_child::<Int16Array>(&batch, 0, 2).len(), 5);
}

#[test]
fn invalid_temporal_values_never_publish_partial_rows() {
    for (scale, ticks) in [(7, TICKS_PER_DAY), (8, 0), (0, 1)] {
        let mut w = writer(&[scaled(TdsDataType::TimeN, scale.min(7))]);
        w.write_time(
            0,
            SqlTime {
                scale,
                time_nanoseconds: ticks,
            },
        );
        assert!(w.check_error().is_err());
        assert!(w.take_batch().is_err());
    }
    for value in [
        SqlDateTime {
            days: -53_691,
            time: 0,
        },
        SqlDateTime {
            days: 2_958_464,
            time: 0,
        },
        SqlDateTime {
            days: 0,
            time: 25_920_000,
        },
        SqlDateTime {
            days: i32::MAX,
            time: u32::MAX,
        },
    ] {
        let mut w = writer(&[fixed(TdsDataType::DateTime)]);
        w.write_datetime(0, value);
        assert_error(&mut w, "outside");
    }
    let mut w = writer(&[fixed(TdsDataType::DateTim4)]);
    w.write_smalldatetime(
        0,
        SqlSmallDateTime {
            days: 0,
            time: 1440,
        },
    );
    assert_error(&mut w, "minutes");
    let mut w = writer(&[scaled(TdsDataType::DateTime2N, 7)]);
    w.write_datetime2(
        0,
        SqlDateTime2 {
            days: MAX_SQL_DAYS + 1,
            time: SqlTime {
                time_nanoseconds: 0,
                scale: 7,
            },
        },
    );
    assert_error(&mut w, "outside");
    for (days, time, offset) in [
        (0, 0, -841),
        (0, 0, 841),
        (0, 0, -1),
        (MAX_SQL_DAYS, TICKS_PER_DAY - 1, 1),
    ] {
        let mut w = writer(&[scaled(TdsDataType::DateTimeOffsetN, 7)]);
        w.write_datetimeoffset(
            0,
            SqlDateTimeOffset {
                datetime2: SqlDateTime2 {
                    days,
                    time: SqlTime {
                        time_nanoseconds: time,
                        scale: 7,
                    },
                },
                offset,
            },
        );
        assert!(w.check_error().is_err());
        assert!(w.take_batch().is_err());
    }
}

#[test]
fn vectors_have_metadata_fixed_dimensions_and_valid_null_children() {
    let mut w = writer(&[vector_meta(3, 0)]);
    w.write_vector(0, SqlVector::try_from_f32(vec![1.25, -2.5, 0.0]).unwrap());
    w.end_row();
    w.write_null(0);
    w.end_row();
    w.check_error().unwrap();
    let batch = w.take_batch().unwrap();
    let list = array::<FixedSizeListArray>(&batch, 0);
    assert_eq!(list.value_length(), 3);
    assert!(list.is_null(1));
    assert_eq!(list.values().len(), 6);
    assert_eq!(list.values().null_count(), 0);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values(),
        &[1.25, -2.5, 0.0, 0.0, 0.0, 0.0]
    );
    assert_eq!(
        batch.schema().field(0).metadata()["mssql.vector.dimensions"],
        "3"
    );
    assert_eq!(
        batch.schema().field(0).metadata()["mssql.vector.base_type"],
        "float32"
    );
    assert_eq!(w.take_batch().unwrap().num_rows(), 0);
    for value in [
        SqlVector::try_from_f32(vec![1.0, 2.0]).unwrap(),
        SqlVector::try_from_f16(vec![1.0, 2.0, 3.0]).unwrap(),
        SqlVector {
            base_type: VectorBaseType::Float32,
            data: VectorData::Float16(vec![1.0, 2.0, 3.0]),
        },
        SqlVector {
            base_type: VectorBaseType::Float32,
            data: VectorData::Float32(vec![]),
        },
    ] {
        let mut w = writer(&[vector_meta(3, 0)]);
        w.write_vector(0, value);
        assert!(w.check_error().is_err());
        assert!(w.take_batch().is_err());
    }
}

#[test]
fn unknown_encrypted_variant_invalid_width_and_scale_schemas_fail() {
    for tds_type in [
        TdsDataType::None,
        TdsDataType::SqlTable,
        TdsDataType::SsVariant,
    ] {
        let mut meta = fixed(TdsDataType::Int4);
        meta.data_type = tds_type;
        meta.type_info.tds_type = tds_type;
        let error = ArrowRowWriter::new(&[meta], ArrowOptions::default())
            .err()
            .unwrap();
        assert!(error.to_string().contains("CAST"));
    }
    let mut meta = fixed(TdsDataType::Int4);
    meta.flags |= 0x800;
    assert!(column_schema(&meta)
        .unwrap_err()
        .to_string()
        .contains("encrypted"));
    for tds_type in [
        TdsDataType::IntN,
        TdsDataType::FltN,
        TdsDataType::MoneyN,
        TdsDataType::DateTimeN,
    ] {
        for width in [0, 3, 16, usize::MAX] {
            assert!(column_schema(&variable(tds_type, width)).is_err());
        }
    }
    assert!(column_schema(&variable(TdsDataType::BitN, 2)).is_err());
    assert!(column_schema(&variable(TdsDataType::Guid, 15)).is_err());
    for (precision, scale) in [(0, 0), (39, 0), (3, 4)] {
        assert!(column_schema(&decimal_meta(precision, scale, false)).is_err());
    }
    for tds_type in [
        TdsDataType::TimeN,
        TdsDataType::DateTime2N,
        TdsDataType::DateTimeOffsetN,
    ] {
        assert!(column_schema(&scaled(tds_type, 8)).is_err());
        let mut meta = scaled(tds_type, 7);
        meta.type_info.length = 0;
        assert!(column_schema(&meta).is_err());
    }
    for (dimensions, base) in [(0, 0), (1999, 0), (3, 1), (3, 255)] {
        assert!(column_schema(&vector_meta(dimensions, base)).is_err());
    }
    let mut meta = vector_meta(3, 0);
    meta.type_info.length -= 1;
    assert!(column_schema(&meta).is_err());
    let mut meta = fixed(TdsDataType::Int4);
    meta.type_info.tds_type = TdsDataType::Flt4;
    assert!(column_schema(&meta).is_err());
}

#[test]
fn callback_mismatch_order_count_and_first_error_remain_safe() {
    let mut w = writer(&[fixed(TdsDataType::Int4)]);
    w.write_i64(0, 1);
    w.write_i32(999, 1);
    w.end_row();
    assert_error(&mut w, "callback type");
    w.check_error().unwrap();
    w.write_i32(0, 1);
    w.end_row();
    assert_eq!(w.rows, 0);
    assert!(w.take_batch().is_err());
    for col in [1, usize::MAX] {
        let mut w = writer(&[fixed(TdsDataType::Int4)]);
        w.write_i32(col, 1);
        assert_error(&mut w, "callback column");
    }
    let mut w = writer(&[fixed(TdsDataType::Int4), fixed(TdsDataType::Int4)]);
    w.write_i32(0, 1);
    w.write_i32(0, 1);
    assert_error(&mut w, "expected 1");
    let mut w = writer(&[fixed(TdsDataType::Int4)]);
    w.end_row();
    assert_error(&mut w, "row has 0 columns");
    let mut w = writer(&[fixed(TdsDataType::Int4)]);
    w.write_i32(0, 1);
    assert!(w
        .take_batch()
        .unwrap_err()
        .to_string()
        .contains("unfinished"));
    w.end_row();
    assert!(w.take_batch().is_err());
    let mut nonnullable = fixed(TdsDataType::Int4);
    nonnullable.flags = 0;
    let mut w = writer(&[nonnullable]);
    w.write_null(0);
    assert_error(&mut w, "non-nullable");
    let mut w = writer(&[fixed(TdsDataType::Int4)]);
    w.write_variant_base_type(0, TdsDataType::Int4);
    assert_error(&mut w, "sql_variant");
}

#[test]
fn hard_value_limits_check_raw_and_expanded_output_before_heap_copy() {
    let limited = |value| ArrowOptions {
        max_value_bytes: Some(value),
        ..ArrowOptions::default()
    };
    let mut w = ArrowRowWriter::new(&[text_meta()], limited(3)).unwrap();
    w.write_string(0, Cow::Borrowed(&[b'a', 0, b'b', 0]), EncodingType::Utf16);
    assert_eq!(w.scratch.capacity(), 0);
    assert_error(&mut w, "max_value_bytes");
    let mut w = ArrowRowWriter::new(&[text_meta()], limited(2)).unwrap();
    w.write_string(0, Cow::Borrowed(&[0, 0xd8]), EncodingType::Utf16);
    assert_eq!(w.scratch.capacity(), 0);
    assert_error(&mut w, "max_value_bytes");
    let mut w =
        ArrowRowWriter::new(&[variable(TdsDataType::BigVarBinary, 1024)], limited(3)).unwrap();
    w.write_bytes(0, Cow::Borrowed(&[1, 2, 3, 4]));
    assert_error(&mut w, "max_value_bytes");
    let mut w = ArrowRowWriter::new(&[text_meta()], limited(3)).unwrap();
    w.write_string(0, Cow::Borrowed(b"abc"), EncodingType::Utf8);
    w.end_row();
    assert_eq!(w.last_row_bytes(), 12);
    w.check_error().unwrap();
    assert_eq!(w.take_batch().unwrap().num_rows(), 1);
    let mut w = ArrowRowWriter::new(&[vector_meta(3, 0)], limited(19)).unwrap();
    w.write_vector(0, SqlVector::try_from_f32(vec![1.0, 2.0, 3.0]).unwrap());
    assert_error(&mut w, "max_value_bytes");
    let mut w = ArrowRowWriter::new(&[decimal_meta(38, 0, false)], limited(16)).unwrap();
    w.write_decimal(0, DecimalParts::new(true, 38, 0, 1));
    w.end_row();
    w.check_error().unwrap();
    assert_eq!(
        array::<Decimal128Array>(&w.take_batch().unwrap(), 0).value(0),
        1
    );
}

#[test]
fn decimal_wire_width_is_independent_of_precision_based_table_storage() {
    for (precision, scale, numeric) in [(9, 2, true), (12, 4, false)] {
        for width in [9, 13, 17] {
            let tds_type = if numeric {
                TdsDataType::NumericN
            } else {
                TdsDataType::DecimalN
            };
            let meta = metadata(
                TypeInfo::var_len_precision_scale(tds_type, width, precision, scale).unwrap(),
            );
            let mut w = writer(&[meta]);
            let empty = w.take_batch().unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(
                empty.schema().field(0).data_type(),
                &DataType::Decimal128(precision, scale as i8)
            );
            assert_eq!(
                empty.schema().field(0).metadata()["mssql.length"],
                width.to_string()
            );
            let value = DecimalParts::new(false, precision, scale, 1_234_567);
            if numeric {
                w.write_numeric(0, value);
            } else {
                w.write_decimal(0, value);
            }
            w.end_row();
            w.write_null(0);
            w.end_row();
            w.check_error().unwrap();
            let batch = w.take_batch().unwrap();
            assert_eq!(array::<Decimal128Array>(&batch, 0).value(0), -1_234_567);
            assert!(array::<Decimal128Array>(&batch, 0).is_null(1));
        }
    }
    for width in [0, 1, 4, 6, 18, usize::MAX] {
        let meta = metadata(
            TypeInfo::var_len_precision_scale(TdsDataType::DecimalN, width, 12, 4).unwrap(),
        );
        assert!(column_schema(&meta).is_err());
    }
}

#[test]
fn limited_transcoding_handles_multiple_chunks_without_rejecting_valid_text() {
    let expected = "a".repeat(10_000);
    let bytes = utf16(&expected);
    for max_row_bytes in [Some(expected.len() + 9), None] {
        let options = ArrowOptions {
            max_row_bytes,
            max_value_bytes: Some(bytes.len()),
            ..ArrowOptions::default()
        };
        let mut w = ArrowRowWriter::new(&[text_meta()], options).unwrap();
        w.write_string(0, Cow::Borrowed(&bytes), EncodingType::Utf16);
        w.end_row();
        w.check_error().unwrap();
        assert_eq!(
            array::<LargeStringArray>(&w.take_batch().unwrap(), 0).value(0),
            expected
        );
    }
    let options = ArrowOptions {
        max_row_bytes: Some(expected.len() + 8),
        ..ArrowOptions::default()
    };
    let mut w = ArrowRowWriter::new(&[text_meta()], options).unwrap();
    w.write_string(0, Cow::Borrowed(&bytes), EncodingType::Utf16);
    assert_error(&mut w, "max_row_bytes");
    assert!(w.scratch.len() < expected.len());
}

#[test]
fn nullable_money_fixed_scalars_and_explicit_null_schema_mapping() {
    for (metadata, kind) in [
        (variable(TdsDataType::MoneyN, 4), Kind::SmallMoney),
        (variable(TdsDataType::MoneyN, 8), Kind::Money),
        (variable(TdsDataType::BitN, 1), Kind::Bool),
        (fixed(TdsDataType::Int1), Kind::U8),
        (fixed(TdsDataType::Int2), Kind::I16),
        (fixed(TdsDataType::Int4), Kind::I32),
        (fixed(TdsDataType::Int8), Kind::I64),
        (fixed(TdsDataType::Flt4), Kind::F32),
        (fixed(TdsDataType::Flt8), Kind::F64),
    ] {
        assert_eq!(column_schema(&metadata).unwrap().0, kind);
    }
    let mut metadata = fixed(TdsDataType::Int4);
    metadata.data_type = TdsDataType::Void;
    metadata.type_info.tds_type = TdsDataType::Void;
    metadata.type_info.length = 0;
    let mut w = writer(&[metadata]);
    w.write_null(0);
    w.end_row();
    w.check_error().unwrap();
    let batch = w.take_batch().unwrap();
    assert_eq!(batch.column(0).data_type(), &DataType::Null);
    assert_eq!(batch.column(0).len(), 1);
}

#[test]
fn hard_row_limit_accounts_for_offsets_null_slots_and_all_columns() {
    let options = ArrowOptions {
        max_row_bytes: Some(17),
        ..ArrowOptions::default()
    };
    let metadata = [fixed(TdsDataType::Int4), text_meta()];
    let mut w = ArrowRowWriter::new(&metadata, options.clone()).unwrap();
    w.write_i32(0, 1);
    w.write_string(1, Cow::Borrowed(b"abc"), EncodingType::Utf8);
    w.end_row();
    assert_eq!(w.last_row_bytes(), 17);
    w.check_error().unwrap();
    w.write_null(0);
    w.write_string(1, Cow::Borrowed(b"abcd"), EncodingType::Utf8);
    assert_error(&mut w, "max_row_bytes");
    let mut w = ArrowRowWriter::new(
        &[text_meta()],
        ArrowOptions {
            max_row_bytes: Some(11),
            ..options
        },
    )
    .unwrap();
    w.write_string(0, Cow::Borrowed(&[0, 0xd8]), EncodingType::Utf16);
    assert_eq!(w.scratch.capacity(), 0);
    assert_error(&mut w, "max_row_bytes");
}

#[test]
fn soft_bytes_allow_one_row_overshoot_and_oversized_rows_are_sliceable() {
    let options = ArrowOptions {
        batch_bytes: 16,
        ..ArrowOptions::default()
    };
    let mut w = ArrowRowWriter::new(&[text_meta()], options).unwrap();
    w.write_string(0, Cow::Borrowed(b"a"), EncodingType::Utf8);
    w.end_row();
    assert!(!w.should_flush());
    w.write_string(0, Cow::Borrowed(b"an oversized row"), EncodingType::Utf8);
    w.end_row();
    w.check_error().unwrap();
    assert!(w.should_flush());
    assert!(w.last_row_bytes() > 16);
    assert_eq!(w.row_count(), 2);
    let batch = w.take_batch().unwrap();
    let prefix = batch.slice(0, 1);
    let oversized = batch.slice(1, 1);
    drop(batch);
    assert_eq!(array::<LargeStringArray>(&prefix, 0).value(0), "a");
    assert_eq!(
        array::<LargeStringArray>(&oversized, 0).value(0),
        "an oversized row"
    );
}

#[test]
fn size_overflow_is_conversion_error_and_poisoned_writer_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<ArrowRowWriter>();
    assert!(checked_add(isize::MAX as usize, 1).is_err());
    assert!(checked_add(usize::MAX, usize::MAX).is_err());
    let mut w = writer(&[fixed(TdsDataType::Int4)]);
    w.bytes = isize::MAX as usize - 1;
    w.write_i32(0, 1);
    assert_error(&mut w, "capacity");
}
