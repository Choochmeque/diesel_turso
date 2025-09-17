use diesel::result::DatabaseErrorInformation;

pub struct TursoError {
    pub(crate) message: String,
}

impl DatabaseErrorInformation for TursoError {
    fn message(&self) -> &str {
        &self.message
    }

    fn details(&self) -> Option<&str> {
        None
    }

    fn hint(&self) -> Option<&str> {
        None
    }

    fn table_name(&self) -> Option<&str> {
        None
    }

    fn column_name(&self) -> Option<&str> {
        None
    }

    fn constraint_name(&self) -> Option<&str> {
        None
    }

    fn statement_position(&self) -> Option<i32> {
        None
    }
}
