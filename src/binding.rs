use std::sync::Arc;
use turso::{Builder, Connection, Database, Value};

#[derive(Debug, Clone)]
pub struct TursoDatabase {
    pub db: Database,
}

#[derive(Debug, Clone)]
pub struct TursoConnection {
    pub conn: Arc<Connection>,
}

#[derive(Debug, Clone)]
pub struct TursoPreparedStatement {
    pub sql: String,
    pub binds: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct TursoResult {
    pub column_names: Arc<[String]>,
    pub rows: Vec<Vec<Value>>,
    pub error: Option<String>,
    pub changes: usize,
}

impl TursoDatabase {
    pub async fn new(path: &str) -> Result<Self, turso::Error> {
        let db = Builder::new_local(path).build().await?;
        Ok(Self { db })
    }

    #[allow(clippy::unused_async)]
    pub async fn connect(&self) -> Result<TursoConnection, turso::Error> {
        let conn = Arc::new(self.db.connect()?);
        Ok(TursoConnection { conn })
    }
}

impl TursoConnection {
    #[allow(clippy::unused_self)]
    pub fn prepare(&self, query: &str) -> TursoPreparedStatement {
        TursoPreparedStatement {
            sql: query.to_string(),
            binds: Vec::new(),
        }
    }

    pub async fn execute(
        &self,
        stmt: &TursoPreparedStatement,
    ) -> Result<TursoResult, turso::Error> {
        // Execute the statement
        let params: Vec<Value> = stmt.binds.clone();
        let result = self.conn.execute(&stmt.sql, params).await;

        // TODO: Workaround: some statements (like PRAGMA) return rows but are called via execute()
        let rows_affected = match result {
            Ok(res) => res,
            Err(turso::Error::Misuse(msg)) if msg.contains("unexpected row") => {
                return self.query(stmt).await;
            }
            Err(e) => return Err(e),
        };

        Ok(TursoResult {
            column_names: Arc::from([]),
            rows: Vec::new(),
            error: None,
            changes: usize::try_from(rows_affected).map_err(|_| {
                turso::Error::ConversionFailure(format!(
                    "rows_affected ({rows_affected}) exceeds usize::MAX"
                ))
            })?,
        })
    }

    pub async fn execute_batch(&self, stmt: &TursoPreparedStatement) -> Result<(), turso::Error> {
        // Execute the statement
        self.conn.execute_batch(&stmt.sql).await?;
        Ok(())
    }

    pub async fn query(&self, stmt: &TursoPreparedStatement) -> Result<TursoResult, turso::Error> {
        let mut prepared = self.conn.prepare(&stmt.sql).await?;
        let params: Vec<Value> = stmt.binds.clone();
        let mut rows_iter = prepared.query(params).await?;
        let column_names: Arc<[String]> = prepared
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        let column_count = column_names.len();

        let mut rows = Vec::new();
        while let Some(row) = rows_iter.next().await? {
            let row_data: Vec<Value> = (0..column_count)
                .map(|idx| row.get_value(idx))
                .collect::<Result<_, _>>()?;
            rows.push(row_data);
        }

        Ok(TursoResult {
            column_names,
            rows,
            error: None,
            changes: 0,
        })
    }
}

impl TursoPreparedStatement {
    pub fn bind(&mut self, values: Vec<Value>) -> &mut Self {
        self.binds = values;
        self
    }
}
