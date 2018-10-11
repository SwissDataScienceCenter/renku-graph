package persistence

import models.Person
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

trait PersonComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent =>

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
  }

  _schemas += persons.schema
}
