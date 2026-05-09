//! Connection configuration builder mirroring tiberius' `Config` API.
//!
//! Use [`Config`] to build connection settings with a fluent API, then pass
//! to [`Client::connect()`](crate::Client::connect) or
//! [`TdsManager::new()`](crate::TdsManager::new) for pooling.
//!
//! # Example
//!
//! ```rust,no_run
//! use mssql_tiberius_bridge::{Config, AuthMethod, EncryptionLevel};
//!
//! let mut cfg = Config::new();
//! cfg.host("db.example.com")
//!    .port(1433)
//!    .database("mydb")
//!    .authentication(AuthMethod::sql_server("sa", "password"))
//!    .encryption(EncryptionLevel::Required)
//!    .trust_cert();
//! ```

#[cfg(any(feature = "integrated-auth-gssapi", feature = "winauth"))]
use mssql_tds::connection::client_context::TdsAuthenticationMethod;
use mssql_tds::connection::client_context::{ClientContext, DriverVersion};
use mssql_tds::core::EncryptionSetting;

/// The driver name sent in the TDS Login7 packet and UserAgent feature extension.
/// Follows the MS driver naming convention (e.g., `MS-TDS`, `MS-PYTHON`).
const DRIVER_NAME: &str = "MS-TIB-BRID";

/// TLS encryption level for the connection.
///
/// Controls whether and how TLS is negotiated with SQL Server.
/// Mirrors tiberius' `EncryptionLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionLevel {
    /// Encrypt login only; data flows unencrypted.
    Off,
    /// Encrypt the entire connection (default).
    On,
    /// Don't request encryption; server may still require it.
    NotSupported,
    /// Require encryption; fail if the server doesn't support it.
    Required,
    /// TDS 8.0 strict mode — encrypts the entire stream including pre-login.
    Strict,
}

/// Authentication method for SQL Server connections.
///
/// # Variants
///
/// - [`SqlServer`](Self::SqlServer) — username/password authentication (most common)
/// - [`Integrated`](Self::Integrated) — Windows SSPI or Kerberos/GSSAPI
#[derive(Debug, Clone)]
pub enum AuthMethod {
    /// SQL Server authentication with username and password.
    SqlServer { user: String, password: String },
    /// Windows Integrated authentication (Kerberos/NTLM).
    ///
    /// Requires either the `integrated-auth-gssapi` feature (Linux/macOS,
    /// pulls GSSAPI / Kerberos via `mssql-tds`) or the `winauth` feature
    /// (Windows, pulls SSPI via `mssql-tds`).
    #[cfg(any(feature = "integrated-auth-gssapi", feature = "winauth"))]
    Integrated,
}

impl AuthMethod {
    /// Create SQL Server authentication credentials.
    pub fn sql_server(user: impl Into<String>, password: impl Into<String>) -> Self {
        AuthMethod::SqlServer {
            user: user.into(),
            password: password.into(),
        }
    }

    /// Create Windows Integrated authentication.
    ///
    /// Requires the `integrated-auth-gssapi` (Linux/macOS) or `winauth`
    /// (Windows) Cargo feature.
    #[cfg(any(feature = "integrated-auth-gssapi", feature = "winauth"))]
    pub fn integrated() -> Self {
        AuthMethod::Integrated
    }
}

/// Fluent connection configuration builder, mirroring tiberius' `Config`.
///
/// All settings have sensible defaults — at minimum you need to set
/// credentials via [`authentication()`](Self::authentication).
///
/// # Defaults
///
/// | Setting | Default |
/// |---------|----------|
/// | Host | `localhost` |
/// | Port | `1433` |
/// | Database | `master` |
/// | Encryption | [`On`](EncryptionLevel::On) |
/// | Trust cert | `false` |
#[derive(Debug, Clone)]
pub struct Config {
    host: String,
    port: u16,
    database: String,
    auth: AuthMethod,
    trust_cert: bool,
    encryption: EncryptionLevel,
    application_name: Option<String>,
    instance_name: Option<String>,
}

impl Config {
    /// Create a new `Config` with defaults (`localhost:1433`, `master` database, empty SQL auth).
    pub fn new() -> Self {
        Config {
            host: "localhost".to_string(),
            port: 1433,
            database: "master".to_string(),
            auth: AuthMethod::SqlServer {
                user: String::new(),
                password: String::new(),
            },
            trust_cert: false,
            encryption: EncryptionLevel::On,
            application_name: None,
            instance_name: None,
        }
    }

    /// Set the server hostname or IP address.
    pub fn host(&mut self, host: impl Into<String>) -> &mut Self {
        self.host = host.into();
        self
    }

    /// Set the server port (default: 1433).
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }

    /// Set the default database to connect to.
    pub fn database(&mut self, database: impl Into<String>) -> &mut Self {
        self.database = database.into();
        self
    }

    /// Set the authentication method (see [`AuthMethod`]).
    pub fn authentication(&mut self, auth: AuthMethod) -> &mut Self {
        self.auth = auth;
        self
    }

    /// Trust the server's TLS certificate without validation.
    ///
    /// **Use only for development** — in production, configure proper
    /// certificate validation.
    pub fn trust_cert(&mut self) -> &mut Self {
        self.trust_cert = true;
        self
    }

    /// Set the TLS encryption level.
    pub fn encryption(&mut self, level: EncryptionLevel) -> &mut Self {
        self.encryption = level;
        self
    }

    /// Set the application name reported to the server in `sys.dm_exec_sessions`.
    pub fn application_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.application_name = Some(name.into());
        self
    }

    /// Set the named instance (e.g., `SQLEXPRESS`).
    ///
    /// When set, the client uses SQL Browser to resolve the port.
    pub fn instance_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.instance_name = Some(name.into());
        self
    }

    /// Get the address as `host:port` (useful for logging).
    pub fn get_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Build the TDS datasource string used by `TdsConnectionProvider`.
    ///
    /// Format: `tcp:host,port` or `tcp:host,port\instance`.
    pub fn datasource_string(&self) -> String {
        if let Some(ref inst) = self.instance_name {
            format!("tcp:{},{}\\{}", self.host, self.port, inst)
        } else {
            format!("tcp:{},{}", self.host, self.port)
        }
    }

    /// Convert to an mssql-tds `ClientContext`.
    ///
    /// Sets the driver identity so SQL Server can distinguish connections
    /// from `mssql-tiberius-bridge` in `sys.dm_exec_sessions` and telemetry:
    ///
    /// - **Library name** (Login7 `ibCltIntName`): `MS-TIB-BRID`
    /// - **UserAgent feature extension** (TDS 0x10):
    ///   `1|MS-TIB-BRID|{version}|{arch}|{os}|{os_details}|...`
    /// - **Driver version** (`client_prog_ver`): crate major.minor.build
    pub fn to_client_context(&self) -> ClientContext {
        let mut ctx = ClientContext::default();
        ctx.database = self.database.clone();

        match &self.auth {
            AuthMethod::SqlServer { user, password } => {
                ctx.user_name = user.clone();
                ctx.password = password.clone();
            }
            #[cfg(any(feature = "integrated-auth-gssapi", feature = "winauth"))]
            AuthMethod::Integrated => {
                ctx.tds_authentication_method = TdsAuthenticationMethod::SSPI;
            }
        }

        ctx.encryption_options.trust_server_certificate = self.trust_cert;

        match self.encryption {
            EncryptionLevel::Off => ctx.encryption_options.mode = EncryptionSetting::PreferOff,
            EncryptionLevel::On => ctx.encryption_options.mode = EncryptionSetting::On,
            EncryptionLevel::NotSupported => {
                ctx.encryption_options.mode = EncryptionSetting::PreferOff
            }
            EncryptionLevel::Required => ctx.encryption_options.mode = EncryptionSetting::Required,
            EncryptionLevel::Strict => ctx.encryption_options.mode = EncryptionSetting::Strict,
        }

        if let Some(ref app) = self.application_name {
            ctx.application_name = app.clone();
        }

        // ── Driver identity ──
        // Login7 client interface name
        ctx.library_name = DRIVER_NAME.to_string();
        // Login7 client_prog_ver from the bridge crate version
        ctx.driver_version = DriverVersion::from_cargo_version();

        // UserAgent feature extension (TDS 0x10)
        let bridge_version = env!("CARGO_PKG_VERSION");
        ctx.user_agent.set_library_name(DRIVER_NAME.to_string());
        ctx.user_agent
            .set_driver_version(bridge_version.to_string());
        ctx
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::*;
    #[cfg(any(feature = "integrated-auth-gssapi", feature = "winauth"))]
    use mssql_tds::connection::client_context::TdsAuthenticationMethod;

    #[test]
    fn default_config() {
        let cfg = Config::new();
        assert_eq!(cfg.get_addr(), "localhost:1433");
        assert_eq!(cfg.datasource_string(), "tcp:localhost,1433");
    }

    #[test]
    fn fluent_builder() {
        let mut cfg = Config::new();
        cfg.host("db.example.com")
            .port(1445)
            .database("mydb")
            .authentication(AuthMethod::sql_server("sa", "pass123"))
            .trust_cert()
            .encryption(EncryptionLevel::Required);
        assert_eq!(cfg.get_addr(), "db.example.com:1445");
        assert_eq!(cfg.datasource_string(), "tcp:db.example.com,1445");

        let ctx = cfg.to_client_context();
        assert_eq!(ctx.user_name, "sa");
        assert_eq!(ctx.password, "pass123");
        assert_eq!(ctx.database, "mydb");
        assert!(ctx.encryption_options.trust_server_certificate);
    }

    #[test]
    fn instance_name_in_datasource() {
        let mut cfg = Config::new();
        cfg.host("server").instance_name("SQLEXPRESS");
        assert_eq!(cfg.datasource_string(), "tcp:server,1433\\SQLEXPRESS");
    }

    #[test]
    #[cfg(any(feature = "integrated-auth-gssapi", feature = "winauth"))]
    fn integrated_auth() {
        let mut cfg = Config::new();
        cfg.authentication(AuthMethod::integrated());
        let ctx = cfg.to_client_context();
        assert!(matches!(
            ctx.tds_authentication_method,
            TdsAuthenticationMethod::SSPI
        ));
    }

    #[test]
    fn driver_identity_is_set() {
        let cfg = Config::new();
        let ctx = cfg.to_client_context();

        // Login7 library name
        assert_eq!(ctx.library_name, "MS-TIB-BRID");

        // UserAgent feature extension fields
        assert_eq!(ctx.user_agent.library_name, "MS-TIB-BRID");
        assert_eq!(ctx.user_agent.driver_version, env!("CARGO_PKG_VERSION"));
    }
}
