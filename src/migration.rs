use diesel_migrations::MigrationHarness;
use crate::{AsyncTursoConnection, TursoBackend};

impl MigrationHarness<TursoBackend> for AsyncTursoConnection {
    fn run_migration(&mut self, migration: &dyn diesel::migration::Migration<TursoBackend>)
            -> diesel::migration::Result<diesel::migration::MigrationVersion<'static>> {
       unimplemented!() 
    }

    fn revert_migration(
            &mut self,
            migration: &dyn diesel::migration::Migration<TursoBackend>,
        ) -> diesel::migration::Result<diesel::migration::MigrationVersion<'static>> {
        unimplemented!()
    }

    fn applied_migrations(&mut self) -> diesel::migration::Result<Vec<diesel::migration::MigrationVersion<'static>>> {
        unimplemented!()
    }
}
