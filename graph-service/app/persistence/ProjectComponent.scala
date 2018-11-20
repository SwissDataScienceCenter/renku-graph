package persistence

import models.Project
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

import scala.concurrent.Future

trait ProjectComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ExecutionContextComponent =>

  import profile.api._

  class Projects( tag: Tag ) extends Table[Project]( tag, "PROJECTS" ) {
    // Columns
    def id: Rep[String] = column[String]( "ID", O.PrimaryKey )
    def url: Rep[String] = column[String]( "URL" )

    // *
    def * : ProvenShape[Project] =
      ( id, url ) <> ( ( Project.apply _ ).tupled, Project.unapply )
  }

  object projects extends TableQuery( new Projects( _ ) ) {
    val findById = this.findBy( _.id )

    object lowLevelApi {
      def all: DBIO[Seq[Project]] = {
        projects.result
      }

      def find( id: String ): DBIO[Seq[Project]] = {
        projects.findById( id ).result
      }
    }

    object api {
      def all: Future[Seq[Project]] = {
        db.run( lowLevelApi.all )
      }

      def find( ids: Seq[String] ): Future[Seq[Project]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { id <- ids } yield lowLevelApi.find( id ) )
        } yield seq.flatten
        db.run( dbio )
      }
    }
  }

  _schemas += projects.schema
}
