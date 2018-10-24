package persistence

import java.time.Instant

import models.Activity
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

import scala.concurrent.Future

trait ActivityComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ExecutionContextComponent with ImplicitsComponent =>

  import profile.api._

  class Activities( tag: Tag ) extends Table[Activity]( tag, "ACTIVITIES" ) {
    // Columns
    def id: Rep[String] = column[String]( "ID", O.PrimaryKey )
    def label: Rep[String] = column[String]( "LABEL" )
    //    def commit_sha1: Rep[String] = column[String]("COMMIT_SHA1")
    def endTime: Rep[Instant] = column[Instant]( "END_TIME" )

    // *
    def * : ProvenShape[Activity] =
      //      (id, label, commit_sha1, endTime) <> ((Activity.apply _).tupled, Activity.unapply)
      ( id, label, endTime ) <> ( ( Activity.apply _ ).tupled, Activity.unapply )
  }

  object activities extends TableQuery( new Activities( _ ) ) {
    val findById = this.findBy( _.id )

    object lowLevelApi {
      def all: DBIO[Seq[Activity]] = {
        activities.result
      }

      def find( id: String ): DBIO[Seq[Activity]] = {
        activities.findById( id ).result
      }
    }

    object api {
      def all: Future[Seq[Activity]] = {
        db.run( lowLevelApi.all )
      }

      def find( ids: Seq[String] ): Future[Seq[Activity]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { id <- ids } yield lowLevelApi.find( id ) )
        } yield seq.flatten
        db.run( dbio )
      }
    }
  }

  _schemas += activities.schema
}
