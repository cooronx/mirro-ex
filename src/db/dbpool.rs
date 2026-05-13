use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use clickhouse::Client;
use thiserror::Error;
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

use crate::config::DbConfig;

pub type Result<T> = std::result::Result<T, DbPoolError>;

#[derive(Debug, Error)]
pub enum DbPoolError {
    #[error("db pool size must be greater than 0")]
    InvalidPoolSize,
    #[error("failed to acquire db pool permit")]
    AcquirePermit(#[source] AcquireError),
    #[error("db pool client storage lock is poisoned")]
    ClientStoragePoisoned,
    #[error("db pool clients and semaphore are out of sync")]
    PoolOutOfSync,
}

#[derive(Clone)]
pub struct DbPool {
    inner: Arc<DbPoolInner>,
}

struct DbPoolInner {
    clients: Mutex<Vec<Client>>,
    permits: Arc<Semaphore>,
    size: usize,
}

pub struct PooledClient {
    pool: Arc<DbPoolInner>,
    client: Option<Client>,
    _permit: OwnedSemaphorePermit,
}

impl DbPool {
    pub fn new(config: &DbConfig) -> Result<Self> {
        Self::with_client(config.pool_size, build_client(config))
    }

    pub fn with_client(size: usize, template: Client) -> Result<Self> {
        if size == 0 {
            return Err(DbPoolError::InvalidPoolSize);
        }

        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            clients.push(template.clone());
        }

        Ok(Self {
            inner: Arc::new(DbPoolInner {
                clients: Mutex::new(clients),
                permits: Arc::new(Semaphore::new(size)),
                size,
            }),
        })
    }

    pub async fn get_one(&self) -> Result<PooledClient> {
        let permit = self
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(DbPoolError::AcquirePermit)?;

        let client = {
            let mut clients = self
                .inner
                .clients
                .lock()
                .map_err(|_| DbPoolError::ClientStoragePoisoned)?;

            clients.pop().ok_or(DbPoolError::PoolOutOfSync)?
        };

        Ok(PooledClient {
            pool: Arc::clone(&self.inner),
            client: Some(client),
            _permit: permit,
        })
    }

    pub fn size(&self) -> usize {
        self.inner.size
    }

    pub fn available(&self) -> usize {
        self.inner.permits.available_permits()
    }
}

impl Deref for PooledClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client
            .as_ref()
            .expect("pooled client was already returned to the pool")
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client
            .as_mut()
            .expect("pooled client was already returned to the pool")
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let mut clients = self
                .pool
                .clients
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            clients.push(client);
        }
    }
}

pub fn build_client(config: &DbConfig) -> Client {
    Client::default()
        .with_url(&config.url)
        .with_user(&config.user)
        .with_password(&config.password)
        .with_database(&config.database)
}
