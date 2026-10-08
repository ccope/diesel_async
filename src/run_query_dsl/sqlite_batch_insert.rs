//! Support for batch insert statements on SQLite
//!
//! SQLite does not support the `DEFAULT` keyword in `VALUES` clauses.
//! Diesel therefore classifies batch insert statements for SQLite via
//! [`QueryExecutionStrategy`](diesel::query_builder::QueryExecutionStrategy):
//!
//! * Batch inserts without default values are converted into a single
//!   multi-row insert statement.
//! * Batch inserts that might contain default values are split into one
//!   statement per row, which are executed inside of a transaction.
//!
//! This mirrors the behaviour of diesel's `SqliteConnection`.

use super::methods::{ExecuteWithStrategy, LoadWithStrategy};
use super::utils;
use crate::AsyncConnectionCore;
use diesel::deserialize::FromSqlRow;
use diesel::expression::QueryMetadata;
use diesel::query_builder::sqlite_batch_insert::{
    BatchInsertStrategy, IntoRowStatements, IntoSingleStatement, No, Yes,
};
use diesel::query_builder::{AsQuery, Query, QueryFragment, QueryId, SingleQueryStrategy};
use diesel::query_dsl::CompatibleType;
use diesel::result::QueryResult;
use diesel::sqlite::Sqlite;
use futures_util::{stream, StreamExt};

/// Execute several statements one after another inside of a single transaction
///
/// This is used to execute SQLite batch insert statements that contain
/// default values. It is an implementation detail of
/// [`RunQueryDsl`](crate::RunQueryDsl) and should not be used directly.
#[doc(hidden)]
pub trait ExecuteStatementsInTransaction: AsyncConnectionCore {
    /// Execute all statements and return the total number of affected rows
    fn execute_statements_in_transaction<'conn, 'query, T>(
        &'conn mut self,
        statements: Vec<T>,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + 'query;

    /// Load all statements and return the concatenated rows
    fn load_statements_in_transaction<'conn, 'query, T>(
        &'conn mut self,
        statements: Vec<T>,
    ) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query;
}

impl<C> ExecuteStatementsInTransaction for C
where
    C: std::ops::DerefMut + Send,
    C::Target: ExecuteStatementsInTransaction,
{
    fn execute_statements_in_transaction<'conn, 'query, T>(
        &'conn mut self,
        statements: Vec<T>,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        self.deref_mut()
            .execute_statements_in_transaction(statements)
    }

    fn load_statements_in_transaction<'conn, 'query, T>(
        &'conn mut self,
        statements: Vec<T>,
    ) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        self.deref_mut().load_statements_in_transaction(statements)
    }
}

impl<Conn, T> ExecuteWithStrategy<Conn, Sqlite, BatchInsertStrategy<No>> for T
where
    Conn: AsyncConnectionCore<Backend = Sqlite>,
    T: IntoSingleStatement,
    T::Statement: ExecuteWithStrategy<Conn, Sqlite, SingleQueryStrategy>,
{
    fn execute_with_strategy<'conn, 'query>(
        self,
        conn: &'conn mut Conn,
    ) -> Conn::ExecuteFuture<'conn, 'query>
    where
        Self: 'query,
    {
        self.into_single_statement().execute_with_strategy(conn)
    }
}

impl<Conn, T> ExecuteWithStrategy<Conn, Sqlite, BatchInsertStrategy<Yes>> for T
where
    Conn: ExecuteStatementsInTransaction<Backend = Sqlite>,
    T: IntoRowStatements,
    T::Statement: QueryFragment<Sqlite> + QueryId,
{
    fn execute_with_strategy<'conn, 'query>(
        self,
        conn: &'conn mut Conn,
    ) -> Conn::ExecuteFuture<'conn, 'query>
    where
        Self: 'query,
    {
        conn.execute_statements_in_transaction(self.into_row_statements())
    }
}

impl<'query, Conn, Q, U> LoadWithStrategy<'query, Conn, U, BatchInsertStrategy<No>> for Q
where
    Conn: AsyncConnectionCore<Backend = Sqlite>,
    Q: IntoSingleStatement,
    Q::Statement: LoadWithStrategy<'query, Conn, U, SingleQueryStrategy>,
{
    type LoadFuture<'conn>
        =
        <Q::Statement as LoadWithStrategy<'query, Conn, U, SingleQueryStrategy>>::LoadFuture<'conn>
    where
        Conn: 'conn;

    type Stream<'conn>
        = <Q::Statement as LoadWithStrategy<'query, Conn, U, SingleQueryStrategy>>::Stream<'conn>
    where
        Conn: 'conn;

    fn load_with_strategy(self, conn: &mut Conn) -> Self::LoadFuture<'_> {
        self.into_single_statement().load_with_strategy(conn)
    }
}

impl<'query, Conn, Q, U, ST> LoadWithStrategy<'query, Conn, U, BatchInsertStrategy<Yes>> for Q
where
    Conn: ExecuteStatementsInTransaction<Backend = Sqlite>,
    Q: IntoRowStatements,
    Q::Statement: Query + QueryFragment<Sqlite> + QueryId + Send + 'query,
    <Q::Statement as Query>::SqlType: CompatibleType<U, Sqlite, SqlType = ST>,
    U: FromSqlRow<ST, Sqlite> + Send + 'static,
    Sqlite: QueryMetadata<<Q::Statement as Query>::SqlType>,
    ST: 'static,
{
    type LoadFuture<'conn>
        = utils::MapOk<Conn::LoadFuture<'conn, 'query>, Self::Stream<'conn>>
    where
        Conn: 'conn;

    type Stream<'conn>
        = stream::Map<
        Conn::Stream<'conn, 'query>,
        fn(QueryResult<Conn::Row<'conn, 'query>>) -> QueryResult<U>,
    >
    where
        Conn: 'conn;

    fn load_with_strategy(self, conn: &mut Conn) -> Self::LoadFuture<'_> {
        utils::MapOk::new(
            conn.load_statements_in_transaction(self.into_row_statements()),
            |stream| {
                stream.map(|row| {
                    U::build_from_row(&row?).map_err(diesel::result::Error::DeserializationError)
                })
            },
        )
    }
}
