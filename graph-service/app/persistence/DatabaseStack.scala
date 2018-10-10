package persistence

import play.api.db.slick.HasDatabaseConfig
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

abstract class DatabaseStack(
    protected val dbConfig: DatabaseConfig[JdbcProfile]
) extends HasDatabaseConfig[JdbcProfile]
  with SchemasComponent
  with ImplicitsComponent
  with ActivityComponent
  with EntityComponent
  with GenerationComponent
