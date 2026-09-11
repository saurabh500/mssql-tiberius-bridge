use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub(crate) type TiberiusClient = tiberius::Client<Compat<TcpStream>>;

pub(crate) const SELECT: &str = "SELECT id, c2, value, c4 FROM #throughput_rows";

pub(crate) struct ConnectionSettings {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl ConnectionSettings {
    pub(crate) fn from_env() -> Self {
        Self {
            host: std::env::var("BENCH_DB_HOST").unwrap_or_else(|_| "localhost".into()),
            port: std::env::var("BENCH_DB_PORT")
                .map(|port| port.parse().expect("BENCH_DB_PORT must be a u16"))
                .unwrap_or(1433),
            user: std::env::var("BENCH_DB_USER").unwrap_or_else(|_| "sa".into()),
            password: std::env::var("BENCH_DB_PASSWORD")
                .expect("set BENCH_DB_PASSWORD to run the benchmark"),
            database: std::env::var("BENCH_DB_NAME").unwrap_or_else(|_| "master".into()),
        }
    }

    pub(crate) async fn bridge(&self) -> Client {
        let mut config = Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .database(&self.database)
            .authentication(AuthMethod::sql_server(&self.user, &self.password))
            .encryption(EncryptionLevel::Required)
            .trust_cert();
        Client::connect(&config).await.expect("bridge connection")
    }

    pub(crate) async fn tiberius(&self) -> TiberiusClient {
        let mut config = tiberius::Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.authentication(tiberius::AuthMethod::sql_server(&self.user, &self.password));
        config.encryption(tiberius::EncryptionLevel::Required);
        config.trust_cert();
        let tcp = TcpStream::connect((&*self.host, self.port))
            .await
            .expect("Tiberius TCP connection");
        tcp.set_nodelay(true).expect("TCP_NODELAY");
        tiberius::Client::connect(config, tcp.compat_write())
            .await
            .expect("Tiberius connection")
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Shape {
    Numeric,
    Mixed,
}

impl Shape {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Numeric => "numeric",
            Self::Mixed => "mixed",
        }
    }

    pub(crate) fn setup_sql(self, rows: u32) -> String {
        let (types, values) = match self {
            Self::Numeric => (
                "c2 BIGINT NULL, value FLOAT NOT NULL, c4 BIT NOT NULL",
                "CASE WHEN n % 10 != 0 THEN n * 1000000 END, n * 1.5, n % 2",
            ),
            Self::Mixed => (
                "c2 NVARCHAR(100) NULL, value FLOAT NOT NULL, c4 VARBINARY(32) NULL",
                "CASE WHEN n % 10 != 0 THEN \
                 CONCAT(N'row_', n, N'_caf', NCHAR(233), N'_', NCHAR(20013), NCHAR(25991)) END, \
                 n * 1.5, \
                 CASE WHEN n % 10 != 0 THEN CONVERT(VARBINARY(32), CONCAT('payload_', n)) END",
            ),
        };
        format!(
            "SET NOCOUNT ON;
             CREATE TABLE #throughput_rows (id INT NOT NULL, {types});
             WITH digits(n) AS (
                 SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) d(n)
             ), numbers(n) AS (
                 SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
                 FROM digits a CROSS JOIN digits b CROSS JOIN digits c
                 CROSS JOIN digits d CROSS JOIN digits e CROSS JOIN digits f
             )
             INSERT INTO #throughput_rows SELECT n, {values} FROM numbers;"
        )
    }
}

pub(crate) fn row_counts() -> Vec<u32> {
    std::env::var("BENCH_ROW_COUNTS")
        .unwrap_or_else(|_| "100000,1000000".into())
        .split(',')
        .map(|value| {
            let rows = value
                .parse()
                .expect("BENCH_ROW_COUNTS must be comma-separated u32s");
            assert!(
                (1..=1_000_000).contains(&rows),
                "row counts must be 1..=1000000"
            );
            rows
        })
        .collect()
}

pub(crate) fn reverse_methods<T>(methods: &mut [T]) {
    match std::env::var("BENCH_REVERSE").as_deref() {
        Ok("1") => methods.reverse(),
        Ok("0") | Err(std::env::VarError::NotPresent) => {}
        _ => panic!("BENCH_REVERSE must be 0 or 1"),
    }
}

macro_rules! unsupported_values {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(&mut self, _col: usize, _value: $ty) {
            panic!(concat!("unexpected fixture type: ", stringify!($method)));
        })*
    };
}

pub(crate) use unsupported_values;
