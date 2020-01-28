use rusqlite;
use postgres;


use postgres::types::ToSql as PostgresToSql;
use rusqlite::types::ToSql as Sqlite3ToSql;


quick_error! {
    #[derive(Debug)]
    pub enum DatabaseError {
        Postgres(err: postgres::error::Error) {
            from()
            description(err.description())
        }
        Sqlite(err: rusqlite::Error) {
            from()
            description(err.description())
        }
    }
}

pub trait PostgresConnectionExt {
    fn query_rows<T, F>(&mut self, query: &str, values: &[&PostgresToSql], func: F)
    -> Result<Vec<T>, DatabaseError>
        where F: FnMut(postgres::rows::Row) -> T;
}

impl PostgresConnectionExt for postgres::Connection {
    fn query_rows<T, F>(&mut self, query: &str, values: &[&PostgresToSql], func: F)
    -> Result<Vec<T>, DatabaseError>
        where F: FnMut(postgres::rows::Row) -> T
    {
        let rows = (self as &mut postgres::Connection).query(query, values)?;

        Ok(rows.into_iter().map(func).collect())
    }
}

pub enum DatabaseConnection {
    Postgres(postgres::Connection),
    Sqlite(rusqlite::Connection),
}

impl DatabaseConnection {
    pub fn query<T, F>(&mut self, query: &str, values: &[&ToSql], mut func: F) -> Result<Vec<T>, DatabaseError>
        where F: FnMut(&mut DbRow) -> T
    {
        match *self {
            DatabaseConnection::Postgres(ref mut conn) => {
                // Eww, but we need to force a &[&PostgresToSql]
                let vec: Vec<_> = values.into_iter().map(|t| t.as_postgres()).collect();

                conn.query_rows(query, &vec[..], |v| func(&mut DbRow::Postgres(v)))
            }
            DatabaseConnection::Sqlite(ref mut conn) => {
                let vec: Vec<_> = values.into_iter().map(|t| t.as_sqlite()).collect();

                let mut stmt = conn.prepare(query)?;
                let rows = stmt.query_map(&vec[..], |v| func(&mut DbRow::Sqlite(v)))?;

                // rows is an iterator producing Result<T>, calling .collect() on it
                // can magically produce Result<Vec<T>, _>
                let res: Result<_, _> = rows.collect();
                Ok(res?)
            }
        }
    }
}

pub enum DbRow<'a> {
    Postgres(postgres::rows::Row<'a>),
    Sqlite(&'a rusqlite::Row<'a, 'a>),
}

impl<'a> DbRow<'a> {
    pub fn  get<T: FromSql>(&mut self, idx: u8) -> T {
        match *self {
            DbRow::Postgres(ref mut row) => row.get(idx as usize),
            DbRow::Sqlite(ref mut row) => row.get(idx as usize),
        }
    }
}

pub trait FromSql: postgres::types::FromSql + rusqlite::types::FromSql {}
pub trait ToSql {
    fn as_postgres(&self) -> &PostgresToSql;
    fn as_sqlite(&self) -> &Sqlite3ToSql;
}

impl FromSql for i32 {}
impl FromSql for i64 {}
impl FromSql for String {}
impl<T> FromSql for Option<T> where T: FromSql + Clone {}


macro_rules! create_to_sql {
    ($typ:ty) => {
        impl ToSql for $typ {
            fn as_postgres(&self) -> &PostgresToSql { self }
            fn as_sqlite(&self) -> &Sqlite3ToSql { self }
        }
    }
}

create_to_sql!{ String }
create_to_sql!{ i64 }
create_to_sql!{ i16 }
create_to_sql!{ i32 }
create_to_sql!{ i8 }

impl<'a> ToSql for &'a str {
    fn as_postgres(&self) -> &PostgresToSql { self }
    fn as_sqlite(&self) -> &Sqlite3ToSql { self }
}



#[derive(Clone)]
pub enum DatabaseConnector {
    Postgres(String),
    Sqlite3(String),
}

impl DatabaseConnector {
    pub fn connect(&self) -> DatabaseConnection {
        match *self {
            DatabaseConnector::Postgres(ref conn_str) => {
                DatabaseConnection::Postgres(
                     postgres::Connection::connect(&conn_str[..], postgres::TlsMode::None)
                     .unwrap()
                )
            }
            DatabaseConnector::Sqlite3(ref conn_str) => {
                DatabaseConnection::Sqlite(rusqlite::Connection::open(conn_str).unwrap())
            }
        }
    }
}
