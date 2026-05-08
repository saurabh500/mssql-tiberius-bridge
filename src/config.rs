//! Configuration builder mirroring tiberius' Config API.

use mssql_tds::connection::client_context::{ClientContext, TdsAuthenticationMethod};
use mssql_tds::core::{EncryptionOptions, EncryptionSetting};

/// TLS encryption level for the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionLevel {
    /// Encrypt the login packet only.
    Off,
    /// Encrypt the entire connection.
    On,
    /// Let the server decide.
    NotSupported,
    /// Require encryption.
    Required,
    /// TDS 8.0 strict mode.
    Strict,
}

/// Authentication method.
#[derive(Debug, Clone)]
pub enum AuthMethod {
    /// SQL Server authentication with username and password.
    SqlServer { user: String, password: String },
    /// Windows Integrated authentication (Kerberos/NTLM).
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
    pub fn integrated() -> Self {
        AuthMethod::Integrated
    }
}

/// Fluent connection configuration builder, mirroring tiberius' `Config`.
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
    /// Create a new Config with defaults (localhost:1433, empty SQL auth).
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

    /// Set the server hostname.
    pub fn host(&mut self, host: impl Into<String>) -> &mut Self {
        self.host = host.into();
        self
    }

    /// Set the server port.
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }

    /// Set the default database.
    pub fn database(&mut self, database: impl Into<String>) -> &mut Self {
        self.database = database.into();
        self
    }

    /// Set the authentication method.
    pub fn authentication(&mut self, auth: AuthMethod) -> &mut Self {
        self.auth = auth;
        self
    }

    /// Trust the server certificate (skip validation).
    pub fn trust_cert(&mut self) -> &mut Self {
        self.trust_cert = true;
        self
    }

    /// Set the TLS encryption level.
    pub fn encryption(&mut self, level: EncryptionLevel) -> &mut Self {
        self.encryption = level;
        self
    }

    /// Set the application name reported to the server.
    pub fn application_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.application_name = Some(name.into());
        self
    }

    /// Set the instance name (for named instances via SQL Browser).
    pub fn instance_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.instance_name = Some(name.into());
        self
    }

    /// Get the address string in `host:port` format.
    pub fn get_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Build the TDS datasource string for `TdsConnectionProvider`.
    pub fn datasource_string(&self) -> String {
        if let Some(ref inst) = self.instance_name {
            format!("tcp:{},{}\\{}", self.host, self.port, inst)
        } else {
            format!("tcp:{},{}", self.host, self.port)
        }
    }

    /// Convert to an mssql-tds `ClientContext`.
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
        }

        ctx.encryption_options.trust_server_certificate = self.trust_cert;

        match self.encryption {
            EncryptionLevel::Off => ctx.encryption_options.mode = EncryptionSetting::PreferOff,
            EncryptionLevel::On => ctx.encryption_options.mode = EncryptionSetting::On,
            EncryptionLevel::NotSupported => ctx.encryption_options.mode = EncryptionSetting::PreferOff,
            EncryptionLevel::Required => ctx.encryption_options.mode = EncryptionSetting::Required,
            EncryptionLevel::Strict => ctx.encryption_options.mode = EncryptionSetting::Strict,
        }

        if let Some(ref app) = self.application_name {
            ctx.application_name = app.clone();
        }

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
    fn instance_name_in_datasource() {
        let mut cfg = Config::new();
        cfg.host("server").instance_name("SQLEXPRESS");
        assert_eq!(cfg.datasource_string(), "tcp:server,1433\\SQLEXPRESS");
    }

    #[test]
    fn integrated_auth() {
        let mut cfg = Config::new();
        cfg.authentication(AuthMethod::integrated());
        let ctx = cfg.to_client_context();
        assert!(matches!(ctx.tds_authentication_method, TdsAuthenticationMethod::SSPI));
    }
}
