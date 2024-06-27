use std::{cmp::min, time::Duration, future::Future};

use tokio_postgres::{types::ToSql, NoTls, Row, self as postgres};
use anyhow::{anyhow, Context};
use rand::prelude::*;

// Lock wraps a regular DbConn, only allowing access using the
// `lock` method. This method will acquire the advisory lock before
// allowing access to the database, and then release it afterwards.
//
// We use advisory locks to avoid multiple Reshape instances working
// on the same database as the same time. DbLocker is the only way to
// get a DbConn which ensures that all database access is protected by
// a lock.
//
// Postgres docs on advisory locks:
//   https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS
pub struct Lock {
    client: Postgres,
}

impl Lock {
    // Advisory lock keys in Postgres are 64-bit integers.
    // The key we use was chosen randomly.
    const LOCK_KEY: i64 = 4036779288569897133;

    pub async fn connect(config: &postgres::Config) -> anyhow::Result<Self> {
        let (pg, conn) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            conn.await.unwrap();
        });

        // When running DDL queries that acquire locks, we risk causing a "lock queue".
        // When attempting to acquire a lock, Postgres will wait for any long running queries to complete.
        // At the same time, it will block other queries until the lock has been acquired and released.
        // This has the bad effect of the long-running query blocking other queries because of us, forming
        // a queue of other queries until we release our lock.
        //
        // We set the lock_timeout setting to avoid this. This puts an upper bound for how long Postgres will
        // wait to acquire locks and also the maximum amount of time a long-running query can block other queries.
        // We should also add automatic retries to handle these timeouts gracefully.
        //
        // Reference: https://medium.com/paypal-tech/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680
        //
        // TODO: Make lock_timeout configurable
        pg.simple_query("SET lock_timeout = '1s'").await
            .context("failed to set lock_timeout")?;

        Ok(Self {
            client: Postgres::new(pg),
        })
    }

    pub async fn lock(
        &mut self,
        f: impl FnOnce(&mut Postgres) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.acquire_lock().await?;
        let result = f(&mut self.client);
        self.release_lock().await?;

        result
    }

    async fn acquire_lock(&mut self) -> anyhow::Result<()> {
        let success = self
            .client
            .query(&format!("SELECT pg_try_advisory_lock({})", Self::LOCK_KEY))
            .await?
            .first()
            .ok_or_else(|| anyhow!("unexpectedly failed when acquiring advisory lock"))
            .map(|row| row.get::<'_, _, bool>(0))?;

        if success {
            Ok(())
        } else {
            Err(anyhow!("another instance of Reshape is already running"))
        }
    }

    async fn release_lock(&mut self) -> anyhow::Result<()> {
        self.client
            .query(&format!("SELECT pg_advisory_unlock({})", Self::LOCK_KEY))
            .await?
            .first()
            .ok_or_else(|| anyhow!("unexpectedly failed when releasing advisory lock"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait Connection: Send {
    async fn run(&mut self, query: &str) -> anyhow::Result<()>;

    async fn query(&mut self, query: &str) -> anyhow::Result<Vec<Row>>;

    async fn query_with_params(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> anyhow::Result<Vec<Row>>;

    async fn transaction(&mut self) -> anyhow::Result<Transaction>;
}

pub struct Postgres {
    client: postgres::Client,
}

impl Postgres {
    fn new(client: postgres::Client) -> Self {
        Postgres { client }
    }
}

#[async_trait::async_trait]
impl Connection for Postgres {
    async fn run(&mut self, query: &str) -> anyhow::Result<()> {
        retry_automatically(|| self.client.batch_execute(query)).await?;
        Ok(())
    }

    async fn query(&mut self, query: &str) -> anyhow::Result<Vec<Row>> {
        let rows = retry_automatically(|| self.client.query(query, &[])).await?;
        Ok(rows)
    }

    async fn query_with_params(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> anyhow::Result<Vec<Row>> {
        let rows = retry_automatically(|| self.client.query(query, params)).await?;
        Ok(rows)
    }

    async fn transaction(&mut self) -> anyhow::Result<Transaction> {
        let transaction = self.client.transaction().await?;
        Ok(Transaction { transaction })
    }
}

pub struct Transaction<'a> {
    transaction: postgres::Transaction<'a>,
}

impl Transaction<'_> {
    pub async fn commit(self) -> anyhow::Result<()> {
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> anyhow::Result<()> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Connection for Transaction<'_> {
    async fn run(&mut self, query: &str) -> anyhow::Result<()> {
        self.transaction.batch_execute(query).await?;
        Ok(())
    }

    async fn query(&mut self, query: &str) -> anyhow::Result<Vec<Row>> {
        let rows = self.transaction.query(query, &[]).await?;
        Ok(rows)
    }

    async fn query_with_params(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> anyhow::Result<Vec<Row>> {
        let rows = self.transaction.query(query, params).await?;
        Ok(rows)
    }

    async fn transaction(&mut self) -> anyhow::Result<Transaction> {
        let transaction = self.transaction.transaction().await?;
        Ok(Transaction { transaction })
    }
}

// Retry a database operation with exponential backoff and jitter
async fn retry_automatically<T, F, Fut>(mut f: F) -> Result<T, postgres::Error> where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, postgres::Error>>
{
    const STARTING_WAIT_TIME: u64 = 100;
    const MAX_WAIT_TIME: u64 = 3_200;
    const MAX_ATTEMPTS: u32 = 10;

    let mut rng = rand::rngs::OsRng;
    let mut attempts = 0;
    loop {
        let result = f().await;

        let error = match result {
            Ok(_) => return result,
            Err(err) => err,
        };

        // If we got a database error, we check if it's retryable.
        // If we didn't get a database error, then it's most likely some kind of connection
        // error which should also be retried.
        if let Some(db_error) = error.as_db_error() {
            if !error_retryable(db_error) {
                return Err(error);
            }
        }

        attempts += 1;
        if attempts >= MAX_ATTEMPTS {
            return Err(error);
        }

        // The wait time increases exponentially, starting at 100ms and doubling up to a max of 3.2s.
        let wait_time = min(
            MAX_WAIT_TIME,
            STARTING_WAIT_TIME * u64::pow(2, attempts - 1),
        );

        // The jitter is up to half the wait time
        let jitter: u64 = rng.gen_range(0..wait_time / 2);

        tokio::time::sleep(Duration::from_millis(wait_time + jitter)).await;
    }
}

// Check if a database error can be retried
fn error_retryable(error: &postgres::error::DbError) -> bool {
    // LOCK_NOT_AVAILABLE is caused by lock_timeout being exceeded
    matches!(error.code(), &postgres::error::SqlState::LOCK_NOT_AVAILABLE)
}
