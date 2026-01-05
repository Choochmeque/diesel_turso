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
    pub results: Vec<Vec<(String, Value)>>,
    pub error: Option<String>,
    pub changes: usize,
}

impl TursoDatabase {
    pub async fn new(path: &str) -> Result<Self, turso::Error> {
        let db = Builder::new_local(path).build().await?;
        Ok(TursoDatabase { db })
    }

    pub async fn connect(&self) -> Result<TursoConnection, turso::Error> {
        let conn = Arc::new(self.db.connect()?);
        Ok(TursoConnection { conn })
    }
}

impl TursoConnection {
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
            results: Vec::new(),
            error: None,
            changes: rows_affected as usize,
        })
    }

    pub async fn execute_batch(&self, stmt: &TursoPreparedStatement) -> Result<(), turso::Error> {
        // Execute the statement
        self.conn.execute_batch(&stmt.sql).await?;
        Ok(())
    }

    pub async fn query(&self, stmt: &TursoPreparedStatement) -> Result<TursoResult, turso::Error> {
        // Prepare and execute query
        let mut prepared = self.conn.prepare(&stmt.sql).await?;
        let params: Vec<Value> = stmt.binds.clone();
        let mut rows = prepared.query(params).await?;
        let columns = prepared.columns();

        let mut results = Vec::new();
        while let Some(row) = rows.next().await? {
            let mut row_data = Vec::new();
            let column_count = row.column_count();
            for idx in 0..column_count {
                // TODO: Since turso Row doesn't expose column names in row, let's try to get them
                let col_name = match columns.get(idx) {
                    Some(col) => col.name().to_string(),
                    None => format!("col_{}", idx),
                };
                let value = row.get_value(idx)?;
                row_data.push((col_name, value));
            }
            results.push(row_data);
        }

        Ok(TursoResult {
            results,
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

impl TursoResult {
    pub fn results(&self) -> Option<Vec<Vec<(String, Value)>>> {
        if self.results.is_empty() {
            None
        } else {
            Some(self.results.clone())
        }
    }

    pub fn error(&self) -> Option<String> {
        self.error.clone()
    }

    pub fn meta(&self) -> TursoMeta {
        TursoMeta {
            changes: self.changes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TursoMeta {
    pub changes: usize,
}
