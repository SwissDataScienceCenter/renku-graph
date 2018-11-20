package persistence

import models.Person
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

import scala.concurrent.Future

trait PersonComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ExecutionContextComponent =>

  import profile.api._

  class Persons( tag: Tag ) extends Table[Person]( tag, "PERSONS" ) {
    // Columns
    def id: Rep[String] = column[String]( "ID", O.PrimaryKey )
    def name: Rep[String] = column[String]( "NAME" )
    def email: Rep[String] = column[String]( "EMAIL" )

    // *
    def * : ProvenShape[Person] =
      ( id, name, email ) <> ( ( Person.apply _ ).tupled, Person.unapply )
  }

  object persons extends TableQuery( new Persons( _ ) ) {
    val findById = this.findBy( _.id )

    object lowLevelApi {
      def all: DBIO[Seq[Person]] = {
        persons.result
      }

      def find( id: String ): DBIO[Seq[Person]] = {
        persons.findById( id ).result
      }
    }

    object api {
      def all: Future[Seq[Person]] = {
        db.run( lowLevelApi.all )
      }

      def find( ids: Seq[String] ): Future[Seq[Person]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { id <- ids } yield lowLevelApi.find( id ) )
        } yield seq.flatten
        db.run( dbio )
      }
    }
  }

  _schemas += persons.schema
}
