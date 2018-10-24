package persistence

import java.time.Instant

import models.Entity
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

import scala.concurrent.Future

trait EntityComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ExecutionContextComponent =>

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

    object lowLevelApi {
      def all: DBIO[Seq[Entity]] = {
        entities.result
      }

      def find( id: String ): DBIO[Seq[Entity]] = {
        entities.findById( id ).result
      }
    }

    object api {
      def all: Future[Seq[Entity]] = {
        db.run( lowLevelApi.all )
      }

      def find( ids: Seq[String] ): Future[Seq[Entity]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { id <- ids } yield lowLevelApi.find( id ) )
        } yield seq.flatten
        db.run( dbio )
      }
    }
  }

  _schemas += entities.schema
}
