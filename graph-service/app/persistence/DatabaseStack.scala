package persistence

import play.api.db.slick.HasDatabaseConfig
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

abstract class DatabaseStack(
    protected val dbConfig: DatabaseConfig[JdbcProfile]
) extends HasDatabaseConfig[JdbcProfile]
  with SchemasComponent
  with ExecutionContextComponent
  with ImplicitsComponent
  with ActivityComponent
  with AssociationComponent
  with EntityComponent
  with GenerationComponent
  with PersonComponent
  with UsageComponent
