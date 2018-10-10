package persistence

import java.time.Instant

import models.Entity
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

trait EntityComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent =>

  import profile.api._

  class Entities( tag: Tag ) extends Table[Entity]( tag, "ENTITIES" ) {
    // Columns
    def id: Rep[String] = column[String]( "ID", O.PrimaryKey )
    def path: Rep[String] = column[String]( "PATH" )
    def commit_sha1: Rep[String] = column[String]( "COMMIT_SHA1" )
    def isPlan: Rep[Boolean] = column[Boolean]( "IS_PLAN" )

    // *
    def * : ProvenShape[Entity] =
      ( id, path, commit_sha1, isPlan ) <> ( ( Entity.apply _ ).tupled, Entity.unapply )
  }

  object entities extends TableQuery( new Entities( _ ) ) {
    val findById = this.findBy( _.id )
  }

  _schemas += entities.schema
}
