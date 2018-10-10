package persistence

import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.{ AbstractTable, TableQuery }

import scala.collection.mutable

trait SchemasComponent {
  this: HasDatabaseConfig[JdbcProfile] =>

  final type Schema = profile.SchemaDescription

  protected final lazy val _schemas: mutable.Builder[Schema, Seq[Schema]] =
    Seq.newBuilder[Schema]
  final def schemas: Seq[Schema] = _schemas.result()
}
