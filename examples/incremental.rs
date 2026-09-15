//! Run with TEST_DB_PASSWORD (and optionally TEST_DB_HOST/TEST_DB_PORT).
use std::borrow::Cow;

use mssql_tiberius_bridge::writer::*;
use mssql_tiberius_bridge::{AuthMethod, Client, Config, Error, Result};

#[derive(Default)]
struct IntWriter {
    value: Option<i32>,
    ended: bool,
    failed: bool,
}

impl IntWriter {
    fn finish(&self, decoded: bool) -> Result<Option<i32>> {
        if self.failed || (decoded != self.ended) || (decoded != self.value.is_some()) {
            return Err(Error::Conversion(
                "expected one complete non-NULL int".into(),
            ));
        }
        Ok(self.value)
    }
}

macro_rules! reject {
    ($($method:ident($ty:ty)),* $(,)?) => {$(
        fn $method(&mut self, _: usize, _: $ty) { self.failed = true; }
    )*};
}

impl RowWriter for IntWriter {
    fn write_i32(&mut self, col: usize, value: i32) {
        self.failed |= col != 0 || self.value.is_some() || self.ended;
        self.value = Some(value);
    }
    fn end_row(&mut self) {
        self.failed |= self.ended || self.value.is_none();
        self.ended = true;
    }
    fn write_null(&mut self, _: usize) {
        self.failed = true;
    }
    fn write_string(&mut self, _: usize, _: Cow<'_, [u8]>, _: EncodingType) {
        self.failed = true;
    }
    reject!(
        write_bool(bool),
        write_u8(u8),
        write_i16(i16),
        write_i64(i64),
        write_f32(f32),
        write_f64(f64),
        write_bytes(Cow<'_, [u8]>),
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
    );
}

async fn consume(client: &mut Client) -> Result<()> {
    let read: Result<()> = async {
        let mut on_result = client
            .start_query("SELECT 1 AS n; SELECT 2 AS n", &[])
            .await?;
        let mut buffer = Vec::with_capacity(32);
        while on_result {
            let metadata = client.query_metadata()?;
            if metadata.len() != 1
                || metadata.first().map(|c| c.data_type) != Some(TdsDataType::Int4)
            {
                return Err(Error::Conversion("expected a single int column".into()));
            }
            loop {
                buffer.clear();
                let mut ended = false;
                for _ in 0..32 {
                    let mut writer = IntWriter::default();
                    let decoded = client.next_row_into(&mut writer).await?;
                    match writer.finish(decoded)? {
                        Some(value) => buffer.push(value),
                        None => {
                            ended = true;
                            break;
                        }
                    }
                }
                // Replace this with destination processing. No full-query collection.
                println!("{buffer:?}");
                if ended {
                    break;
                }
            }
            on_result = client.next_result().await?;
        }
        Ok(())
    }
    .await;
    let closed = client.close_query().await;
    read?;
    closed
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config
        .connect_retry_count(0)
        .host(std::env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            std::env::var("TEST_DB_PORT")
                .unwrap_or_else(|_| "1433".into())
                .parse()?,
        )
        .authentication(AuthMethod::sql_server(
            "sa",
            std::env::var("TEST_DB_PASSWORD")?,
        ));
    let mut client = Client::connect(&config).await?;
    consume(&mut client).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversion_errors_and_partial_rows_are_not_success() {
        let mut writer = IntWriter::default();
        writer.write_i32(0, 42);
        assert!(writer.finish(true).is_err());
        writer.end_row();
        assert_eq!(writer.finish(true).expect("complete row"), Some(42));
        writer.write_null(0);
        assert!(writer.finish(true).is_err());
        assert!(writer.finish(false).is_err());
        assert_eq!(IntWriter::default().finish(false).expect("boundary"), None);
    }
}
