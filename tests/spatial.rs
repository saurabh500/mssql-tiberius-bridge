//! Spatial regressions for issue #69. Uses the standard TEST_DB_* environment.

use futures_util::TryStreamExt;
use mssql_tiberius_bridge::{AuthMethod, Client, ColumnType, ColumnValues, Config, Row};

async fn connect() -> Option<Client> {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(password) => password,
        Err(std::env::VarError::NotPresent) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping spatial integration tests");
            return None;
        }
        Err(error) => panic!("invalid TEST_DB_PASSWORD: {error}"),
    };
    let mut config = Config::new();
    config
        .host(std::env::var("TEST_DB_HOST").unwrap_or("localhost".into()))
        .port(
            std::env::var("TEST_DB_PORT")
                .map(|port| port.parse().expect("invalid TEST_DB_PORT"))
                .unwrap_or(1433),
        )
        .database(std::env::var("TEST_DB_NAME").unwrap_or("master".into()))
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or("sa".into()),
            password,
        ))
        .trust_cert();
    Some(
        Client::connect(&config)
            .await
            .expect("connect to SQL Server"),
    )
}

fn assert_spatial_bytes(row: &Row, name: &str, column_type: ColumnType, srid: u32) {
    let index = row.column_index(name).unwrap();
    let column = &row.columns()[index];
    assert_eq!(column.column_type(), column_type);
    assert!(column.is_plp());
    assert_eq!(column.char_length(), None);
    let expected_name = format!("{name}_bytes");
    let expected = row.get::<&[u8], _>(expected_name.as_str());
    assert_eq!(row.try_get::<&[u8], _>(index).unwrap(), expected);
    assert_eq!(row.get::<Vec<u8>, _>(name), expected.map(<[u8]>::to_vec));
    assert_eq!(row.get::<Option<&[u8]>, _>(name), Some(expected));
    match (row.raw_value(index), expected) {
        (Some(ColumnValues::Bytes(bytes)), Some(expected)) => {
            assert_eq!(bytes, expected);
            assert_eq!(&bytes[..4], &srid.to_le_bytes());
            assert_eq!(row.get::<&[u8], _>(name).unwrap().as_ptr(), bytes.as_ptr());
        }
        (Some(ColumnValues::Null), None) => {}
        other => panic!("unexpected spatial value: {other:?}"),
    }
    assert_eq!(row.get::<&str, _>(name), None);
}

const SHAPES: &str = "
    DECLARE @shapes TABLE (id int, g geography, m geometry);
    INSERT INTO @shapes VALUES
        (1, geography::Point(1, 2, 4326), geometry::Point(2, 1, 0)),
        (2, geography::STGeomFromText('LINESTRING(0 0, 1 1)', 4326),
            geometry::STGeomFromText('LINESTRING(0 0, 1 1)', 0)),
        (3, geography::STGeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))', 4326),
            geometry::STGeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))', 0)),
        (4, geography::STGeomFromText('GEOMETRYCOLLECTION EMPTY', 4326),
            geometry::STGeomFromText('GEOMETRYCOLLECTION EMPTY', 0)),
        (5, NULL, NULL),
        (6, geography::STGeomFromText('POINT(2 1 3 4)', 4326),
            geometry::STGeomFromText('CIRCULARSTRING(0 0, 1 1, 2 0)', 0));
    SELECT id, g, m, g.Serialize() AS g_bytes, m.Serialize() AS m_bytes,
           CAST(0x010203 AS varbinary(3)) AS ordinary_bytes, N'after' AS tail
    FROM @shapes ORDER BY id;
";

#[tokio::test]
async fn spatial_buffered_and_streamed_reads_preserve_native_serialization() {
    let Some(mut client) = connect().await else {
        return;
    };
    let buffered = client
        .simple_query(SHAPES)
        .await
        .unwrap()
        .into_first_result();
    let streamed: Vec<Row> = client
        .simple_query_streamed(SHAPES)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(buffered, streamed);
    for rows in [&buffered, &streamed] {
        assert_eq!(rows.len(), 6);
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row.get::<i32, _>("id"), Some(index as i32 + 1));
            assert_spatial_bytes(row, "g", ColumnType::Geography, 4326);
            assert_spatial_bytes(row, "m", ColumnType::Geometry, 0);
            assert_eq!(row.columns()[5].column_type(), ColumnType::VarBinary);
            assert_eq!(row.get::<&[u8], _>("ordinary_bytes"), Some(&[1, 2, 3][..]));
            assert_eq!(row.get::<&str, _>("tail"), Some("after"));
        }
        assert!(
            rows[3].get::<Vec<u8>, _>("g").is_some(),
            "EMPTY is not NULL"
        );
        assert!(
            rows[3].get::<Vec<u8>, _>("m").is_some(),
            "EMPTY is not NULL"
        );
        assert_eq!(rows[4].get::<Vec<u8>, _>("g"), None);
        assert_eq!(rows[4].get::<Vec<u8>, _>("m"), None);
        // A single point: native SRID + version/flags + two coordinates, not WKB.
        assert_eq!(rows[0].get::<&[u8], _>("g").unwrap().len(), 22);
        assert_eq!(rows[0].get::<&[u8], _>("m").unwrap().len(), 22);
    }
    let next = client
        .simple_query("SELECT 42 AS n")
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(next[0].get::<i32, _>("n"), Some(42));
}

#[tokio::test]
async fn spatial_parameterized_reads_span_multiple_packets() {
    let Some(mut client) = connect().await else {
        return;
    };
    let sql = "
        DECLARE @wkt nvarchar(max) = N'LINESTRING(0 1', @i int = 1;
        WHILE @i < @P1
        BEGIN
            SET @wkt += N', ' + CONVERT(nvarchar(30), CAST(@i AS decimal(10,2)) / 100) + N' 1';
            SET @i += 1;
        END;
        SET @wkt += N')';
        DECLARE @g geography = geography::STGeomFromText(@wkt, 4326);
        DECLARE @m geometry = geometry::STGeomFromText(@wkt, 1234);
        SELECT @g AS g, @m AS m, @g.Serialize() AS g_bytes,
               @m.Serialize() AS m_bytes, 42 AS tail;
    ";
    let buffered = client
        .query(sql, &[&2000i32])
        .await
        .unwrap()
        .into_first_result();
    let streamed: Vec<Row> = client
        .query_streamed(sql, &[&2000i32])
        .try_collect()
        .await
        .unwrap();
    assert_eq!(buffered, streamed);
    for rows in [&buffered, &streamed] {
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_spatial_bytes(row, "g", ColumnType::Geography, 4326);
        assert_spatial_bytes(row, "m", ColumnType::Geometry, 1234);
        for name in ["g", "m"] {
            assert!(row.get::<&[u8], _>(name).unwrap().len() > 32_000);
        }
        assert_eq!(row.get::<i32, _>("tail"), Some(42));
    }
}

#[tokio::test]
async fn non_spatial_udt_remains_opaque_bytes() {
    let Some(mut client) = connect().await else {
        return;
    };
    let rows = client
        .simple_query(
            "DECLARE @h hierarchyid = hierarchyid::Parse('/1/2/');
             SELECT @h AS h, CAST(@h AS varbinary(max)) AS h_bytes,
                    CAST(NULL AS hierarchyid) AS missing;",
        )
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.columns()[0].column_type(), ColumnType::Udt);
    assert_eq!(row.columns()[2].column_type(), ColumnType::Udt);
    assert!(matches!(row.raw_value(0), Some(ColumnValues::Bytes(_))));
    assert_eq!(row.get::<&[u8], _>("h"), row.get::<&[u8], _>("h_bytes"));
    assert_eq!(row.get::<Vec<u8>, _>("missing"), None);
}
