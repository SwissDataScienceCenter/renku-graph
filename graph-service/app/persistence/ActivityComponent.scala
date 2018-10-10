package persistence

import java.time.Instant

import models.Activity
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

trait ActivityComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ImplicitsComponent =>

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
  }

  _schemas += activities.schema
}
