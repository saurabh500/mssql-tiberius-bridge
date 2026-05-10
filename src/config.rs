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

use mssql_tds::connection::client_context::{
    ClientContext, DriverVersion, TdsAuthenticationMethod,
};
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
    /// Don't request encryption from the client side. The server may still
    /// require it (TDS pre-login negotiation), in which case the connection
    /// upgrades to TLS regardless.
    ///
    /// **Alias of [`NotSupported`](Self::NotSupported)**: both variants map to
    /// the same underlying `EncryptionSetting::PreferOff` in `mssql-tds`. The
    /// pair exists for parity with tiberius' `EncryptionLevel`; pick whichever
    /// reads better at the call site.
    Off,
    /// Encrypt the entire connection (default).
    On,
    /// Advertise that the client does not support encryption. The server may
    /// still force TLS on the connection.
    ///
    /// **Alias of [`Off`](Self::Off)**: both variants map to
    /// `EncryptionSetting::PreferOff` in `mssql-tds`. Kept for tiberius API
    /// parity.
    NotSupported,
    /// Require encryption; fail if the server doesn't support it.
    Required,
    /// TDS 8.0 strict mode — performs the TLS handshake **before** the TDS
    /// pre-login packet, so the entire wire conversation (including pre-login)
    /// runs inside TLS. This eliminates the pre-login downgrade window and is
    /// required by some Azure SQL configurations (e.g. `Encrypt=Strict` in the
    /// .NET driver).
    ///
    /// Notes specific to `Strict`:
    /// - The TLS handshake uses ALPN with the `tds/8.0` protocol id, as
    ///   required by the TDS 8.0 specification.
    /// - Server certificate validation is **always enforced**; calling
    ///   [`Config::trust_cert`] is ignored under `Strict` mode.
    /// - [`Config::trust_cert_ca`] (custom CA bundle) and
    ///   [`Config::host_name_in_certificate`] (SNI override) still apply.
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
    Integrated,
    /// Microsoft Entra ID (Azure AD) federated authentication using a
    /// pre-acquired access token (JWT). Acquire the token from MSAL /
    /// `azure_identity` / `oauth2`, scoped for `https://database.windows.net/.default`.
    AadToken { token: String },
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
    pub fn integrated() -> Self {
        AuthMethod::Integrated
    }

    /// Create AAD/Entra ID federated authentication from a pre-acquired
    /// access token (JWT). Mirrors tiberius' `AuthMethod::aad_token`.
    pub fn aad_token(token: impl Into<String>) -> Self {
        AuthMethod::AadToken {
            token: token.into(),
        }
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
    server_certificate: Option<String>,
    host_name_in_certificate: Option<String>,
    readonly: bool,
    encryption: EncryptionLevel,
    application_name: Option<String>,
    instance_name: Option<String>,
    client_name: Option<String>,
    send_string_parameters_as_unicode: bool,
    multi_subnet_failover: bool,
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
            server_certificate: None,
            host_name_in_certificate: None,
            readonly: false,
            encryption: EncryptionLevel::On,
            application_name: None,
            instance_name: None,
            client_name: None,
            send_string_parameters_as_unicode: true,
            multi_subnet_failover: false,
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

    /// Pin the server's TLS certificate to the DER- or PEM-encoded X.509
    /// file at `path`.
    ///
    /// When set, the driver bypasses standard CA chain validation and
    /// performs an exact binary match between the file and the certificate
    /// presented by the server. Mirrors tiberius' `Config::trust_cert_ca`.
    ///
    /// # Interaction with [`trust_cert`](Self::trust_cert)
    ///
    /// `trust_cert_ca` **supersedes** `trust_cert`. The bridge forwards both
    /// fields to `mssql-tds`, which logs a warning and uses the pinned
    /// certificate (`ServerCertificate takes precedence`). Net effect:
    /// validation is performed against the pinned cert, and `trust_cert` has
    /// no observable effect when both are set.
    ///
    /// # Mutual exclusion with [`host_name_in_certificate`](Self::host_name_in_certificate)
    ///
    /// `trust_cert_ca` and `host_name_in_certificate` are **mutually
    /// exclusive**: setting both causes `mssql-tds` to fail the TLS handshake
    /// with a usage error. Pin a cert *or* override the SNI hostname, not both.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mssql_tiberius_bridge::Config;
    ///
    /// let mut cfg = Config::new();
    /// cfg.host("db.internal").trust_cert_ca("/etc/ssl/private-ca.pem");
    /// ```
    pub fn trust_cert_ca(&mut self, path: impl Into<String>) -> &mut Self {
        self.server_certificate = Some(path.into());
        self
    }

    /// Override the hostname used for TLS certificate name validation.
    pub fn host_name_in_certificate(&mut self, name: impl Into<String>) -> &mut Self {
        self.host_name_in_certificate = Some(name.into());
        self
    }

    /// Set the client workstation name sent in the Login7 packet.
    pub fn client_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.client_name = Some(name.into());
        self
    }

    /// Controls whether `&str` and `String` parameters are sent as NVARCHAR.
    ///
    /// Defaults to `true`. Set to `false` to send string parameters as VARCHAR.
    pub fn send_string_parameters_as_unicode(&mut self, enabled: bool) -> &mut Self {
        self.send_string_parameters_as_unicode = enabled;
        self
    }

    pub(crate) fn string_parameters_as_unicode(&self) -> bool {
        self.send_string_parameters_as_unicode
    }

    /// Send `ApplicationIntent=ReadOnly` in the login (mirrors tiberius'
    /// `Config::readonly`). Required for routing to a readable secondary in
    /// an Always On availability group / Azure SQL geo-replica.
    ///
    /// Defaults to `ReadWrite`.
    pub fn readonly(&mut self, readonly: bool) -> &mut Self {
        self.readonly = readonly;
        self
    }

    /// Set the TLS encryption level.
    pub fn encryption(&mut self, level: EncryptionLevel) -> &mut Self {
        self.encryption = level;
        self
    }

    /// Enable `MultiSubnetFailover` (MSF) connection mode.
    ///
    /// With SQL Server Always On Availability Groups whose listener spans
    /// multiple subnets, the listener DNS record contains every replica IP.
    /// With MSF enabled, the client resolves all A/AAAA records and races
    /// `TcpStream::connect` against them in parallel, taking the first that
    /// completes the TCP handshake. This drives sub-second failover when a
    /// replica becomes unreachable.
    ///
    /// Notes:
    /// - MSF is only meaningful for **TCP** connections. mssql-tds rejects
    ///   the combination with Named Pipes / Shared Memory / LocalDB.
    /// - Off by default (single-target connect, the typical case).
    /// - Mirrors Microsoft .NET's `MultiSubnetFailover=true` connection
    ///   string keyword.
    pub fn multi_subnet_failover(&mut self, enabled: bool) -> &mut Self {
        self.multi_subnet_failover = enabled;
        self
    }

    /// Returns whether `MultiSubnetFailover` is enabled on this config.
    pub fn is_multi_subnet_failover(&self) -> bool {
        self.multi_subnet_failover
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
    /// Format:
    /// - `tcp:host,port` (default — explicit port)
    /// - `tcp:host\instance` (when [`instance_name`](Self::instance_name) is
    ///   set — port is omitted so `mssql-tds` runs SSRP / SQL Browser to
    ///   resolve the instance's TCP port)
    ///
    /// Per MDAC convention, including both an explicit port and an instance
    /// name causes the instance to be silently ignored, so the bridge drops
    /// the port whenever an instance is specified. If you need a fixed port,
    /// don't call [`instance_name`](Self::instance_name).
    pub fn datasource_string(&self) -> String {
        if let Some(ref inst) = self.instance_name {
            format!("tcp:{}\\{}", self.host, inst)
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
            AuthMethod::Integrated => {
                ctx.tds_authentication_method = TdsAuthenticationMethod::SSPI;
            }
            AuthMethod::AadToken { token } => {
                ctx.tds_authentication_method = TdsAuthenticationMethod::AccessToken;
                ctx.access_token = Some(token.clone());
            }
        }

        ctx.encryption_options.trust_server_certificate = self.trust_cert;
        ctx.encryption_options.server_certificate = self.server_certificate.clone();
        ctx.encryption_options.host_name_in_cert = self.host_name_in_certificate.clone();

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

        if let Some(ref client_name) = self.client_name {
            ctx.workstation_id = client_name.clone();
        }

        // ── Driver identity ──
        // Login7 client interface name
        ctx.library_name = DRIVER_NAME.to_string();
        // Login7 client_prog_ver from the bridge crate version
        ctx.driver_version = DriverVersion::from_cargo_version();

        ctx.application_intent = if self.readonly {
            mssql_tds::message::login_options::ApplicationIntent::ReadOnly
        } else {
            mssql_tds::message::login_options::ApplicationIntent::ReadWrite
        };

        ctx.multi_subnet_failover = self.multi_subnet_failover;

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
    fn trust_cert_ca_sets_server_certificate() {
        let mut cfg = Config::new();
        cfg.trust_cert_ca("/etc/ssl/ca.pem");
        let ctx = cfg.to_client_context();
        assert_eq!(
            ctx.encryption_options.server_certificate.as_deref(),
            Some("/etc/ssl/ca.pem")
        );
        assert!(!ctx.encryption_options.trust_server_certificate);
    }

    #[test]
    fn trust_cert_ca_independent_of_trust_cert() {
        let mut cfg = Config::new();
        cfg.trust_cert().trust_cert_ca("/tmp/ca.pem");
        let ctx = cfg.to_client_context();
        assert!(ctx.encryption_options.trust_server_certificate);
        assert_eq!(
            ctx.encryption_options.server_certificate.as_deref(),
            Some("/tmp/ca.pem")
        );
    }

    #[test]
    fn readonly_sets_application_intent() {
        use mssql_tds::message::login_options::ApplicationIntent;
        let mut cfg = Config::new();
        assert_eq!(
            cfg.to_client_context().application_intent,
            ApplicationIntent::ReadWrite
        );
        cfg.readonly(true);
        assert_eq!(
            cfg.to_client_context().application_intent,
            ApplicationIntent::ReadOnly
        );
        cfg.readonly(false);
        assert_eq!(
            cfg.to_client_context().application_intent,
            ApplicationIntent::ReadWrite
        );
    }

    #[test]
    fn instance_name_in_datasource() {
        let mut cfg = Config::new();
        cfg.host("server").instance_name("SQLEXPRESS");
        // Port is omitted when an instance name is set so mssql-tds runs SSRP.
        assert_eq!(cfg.datasource_string(), "tcp:server\\SQLEXPRESS");
    }

    #[test]
    fn instance_name_overrides_port_in_datasource() {
        let mut cfg = Config::new();
        cfg.host("server").port(14330).instance_name("SQLEXPRESS");
        // Even with an explicit port, instance triggers SSRP; port is dropped.
        assert_eq!(cfg.datasource_string(), "tcp:server\\SQLEXPRESS");
    }

    #[test]
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
    fn aad_token_sets_access_token() {
        let mut cfg = Config::new();
        cfg.authentication(AuthMethod::aad_token("eyJ0eXAi.fake.jwt"));
        let ctx = cfg.to_client_context();
        assert!(matches!(
            ctx.tds_authentication_method,
            TdsAuthenticationMethod::AccessToken
        ));
        assert_eq!(ctx.access_token.as_deref(), Some("eyJ0eXAi.fake.jwt"));
        // user_name/password must remain empty when using AAD
        assert!(ctx.user_name.is_empty());
        assert!(ctx.password.is_empty());
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

    #[test]
    fn client_name_sets_workstation_id() {
        let mut cfg = Config::new();
        cfg.client_name("app-host-01");
        assert_eq!(cfg.to_client_context().workstation_id, "app-host-01");
    }

    #[test]
    fn default_client_name_comes_from_mssql_tds() {
        let cfg = Config::new();
        assert_eq!(
            cfg.to_client_context().workstation_id,
            ClientContext::default().workstation_id
        );
    }

    #[test]
    fn host_name_in_certificate_sets_tls_override() {
        let mut cfg = Config::new();
        cfg.host_name_in_certificate("sql.example.com");
        assert_eq!(
            cfg.to_client_context()
                .encryption_options
                .host_name_in_cert
                .as_deref(),
            Some("sql.example.com")
        );
    }

    #[test]
    fn send_string_parameters_as_unicode_defaults_to_true() {
        let mut cfg = Config::new();
        assert!(cfg.string_parameters_as_unicode());
        cfg.send_string_parameters_as_unicode(false);
        assert!(!cfg.string_parameters_as_unicode());
    }

    #[test]
    fn encryption_level_strict_maps_to_strict() {
        use mssql_tds::core::EncryptionSetting;
        let mut cfg = Config::new();
        cfg.encryption(EncryptionLevel::Strict);
        assert_eq!(
            cfg.to_client_context().encryption_options.mode,
            EncryptionSetting::Strict
        );
    }

    #[test]
    fn encryption_level_required_maps_to_required() {
        use mssql_tds::core::EncryptionSetting;
        let mut cfg = Config::new();
        cfg.encryption(EncryptionLevel::Required);
        assert_eq!(
            cfg.to_client_context().encryption_options.mode,
            EncryptionSetting::Required
        );
    }

    #[test]
    fn encryption_level_on_maps_to_on() {
        use mssql_tds::core::EncryptionSetting;
        let mut cfg = Config::new();
        cfg.encryption(EncryptionLevel::On);
        assert_eq!(
            cfg.to_client_context().encryption_options.mode,
            EncryptionSetting::On
        );
    }

    #[test]
    fn encryption_level_off_maps_to_prefer_off() {
        use mssql_tds::core::EncryptionSetting;
        let mut cfg = Config::new();
        cfg.encryption(EncryptionLevel::Off);
        assert_eq!(
            cfg.to_client_context().encryption_options.mode,
            EncryptionSetting::PreferOff
        );
    }

    #[test]
    fn encryption_level_not_supported_maps_to_prefer_off() {
        use mssql_tds::core::EncryptionSetting;
        let mut cfg = Config::new();
        cfg.encryption(EncryptionLevel::NotSupported);
        assert_eq!(
            cfg.to_client_context().encryption_options.mode,
            EncryptionSetting::PreferOff
        );
    }

    #[test]
    fn strict_preserves_host_name_in_certificate_and_ca() {
        // Strict mode must still honour SNI override and custom CA bundle —
        // only `trust_cert` (TrustServerCertificate) is ignored by mssql-tds
        // under Strict.
        let mut cfg = Config::new();
        cfg.encryption(EncryptionLevel::Strict)
            .host_name_in_certificate("sql.example.com")
            .trust_cert_ca("/etc/ssl/ca.pem");
        let opts = cfg.to_client_context().encryption_options;
        assert_eq!(opts.mode, mssql_tds::core::EncryptionSetting::Strict);
        assert_eq!(opts.host_name_in_cert.as_deref(), Some("sql.example.com"));
        assert_eq!(opts.server_certificate.as_deref(), Some("/etc/ssl/ca.pem"));
    }

    #[test]
    fn multi_subnet_failover_defaults_to_off() {
        let cfg = Config::new();
        assert!(!cfg.is_multi_subnet_failover());
        assert!(!cfg.to_client_context().multi_subnet_failover);
    }

    #[test]
    fn multi_subnet_failover_setter_propagates() {
        let mut cfg = Config::new();
        cfg.multi_subnet_failover(true);
        assert!(cfg.is_multi_subnet_failover());
        assert!(cfg.to_client_context().multi_subnet_failover);

        cfg.multi_subnet_failover(false);
        assert!(!cfg.is_multi_subnet_failover());
        assert!(!cfg.to_client_context().multi_subnet_failover);
    }
}
