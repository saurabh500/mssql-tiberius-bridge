//! Connection pooling via [`deadpool`], mirroring the tiberius + deadpool pattern.
//!
//! # Example
//!
//! ```rust,no_run
//! use mssql_tiberius_bridge::{Config, AuthMethod, TdsManager};
//!
//! # fn example() -> std::result::Result<(), Box<dyn std::error::Error>> {
//! let mut cfg = Config::new();
//! cfg.host("localhost").authentication(AuthMethod::sql_server("sa", "pass")).trust_cert();
//! let pool = TdsManager::create_pool(cfg, 10)?;
//! // Use pool.get().await? to checkout connections
//! # Ok(())
//! # }
//! ```

use deadpool::managed::{Manager, Metrics, RecycleError, RecycleResult};

use crate::client::Client;
use crate::config::Config;
use crate::error::Error;

/// A checked-out connection from the pool.
pub type PooledConnection = deadpool::managed::Object<TdsManager>;

/// Connection pool type alias.
pub type Pool = deadpool::managed::Pool<TdsManager>;

/// [`deadpool::managed::Manager`] implementation for mssql-tds connections.
///
/// Creates and recycles [`Client`] connections using the provided [`Config`].
#[derive(Debug, Clone)]
pub struct TdsManager {
    config: Config,
}

impl TdsManager {
    /// Create a new pool manager from a Config.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Build a [`Pool`] with the given maximum connection count.
    ///
    /// # Errors
    ///
    /// Returns a build error if the pool configuration is invalid.
    pub fn create_pool(
        config: Config,
        max_size: usize,
    ) -> Result<Pool, deadpool::managed::BuildError> {
        let mgr = TdsManager::new(config);
        Pool::builder(mgr).max_size(max_size).build()
    }
}

impl Manager for TdsManager {
    type Type = Client;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Client::connect(&self.config).await
    }

    async fn recycle(&self, conn: &mut Self::Type, _: &Metrics) -> RecycleResult<Self::Error> {
        conn.ping().await.map_err(RecycleError::Backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_is_clone_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        fn assert_clone<T: Clone>() {}
        assert_send::<TdsManager>();
        assert_sync::<TdsManager>();
        assert_clone::<TdsManager>();
    }

    #[test]
    fn create_pool_builder() {
        let cfg = Config::new();
        let pool = TdsManager::create_pool(cfg, 10);
        assert!(pool.is_ok());
    }
}
