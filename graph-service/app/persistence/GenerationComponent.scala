package persistence

import models.{ Activity, Entity, GenerationEdge }
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.{ ForeignKeyQuery, Index, ProvenShape }

trait GenerationComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ActivityComponent with EntityComponent =>

  import profile.api._

  class Generations( tag: Tag )
    extends Table[GenerationEdge]( tag, "GENERATION_EDGES" ) {
    // Columns
    def entityId: Rep[String] = column[String]( "ENTITY" )
    def activityId: Rep[String] = column[String]( "ACTIVITY" )

    // Foreign Keys
    def entity: ForeignKeyQuery[Entities, Entity] =
      foreignKey( "FK_GENERATION_ENTITY", entityId, entities )(
        _.id,
        onUpdate = ForeignKeyAction.Cascade,
        onDelete = ForeignKeyAction.Restrict
      )
    def activity: ForeignKeyQuery[Activities, Activity] =
      foreignKey( "FK_GENERATION_ACTIVITY", activityId, activities )(
        _.id,
        onUpdate = ForeignKeyAction.Cascade,
        onDelete = ForeignKeyAction.Restrict
      )

    // Indexes
    def idxEntity: Index =
      index( "IDX_GENERATION_ENTITY", entityId, unique = true )
    def idxActivity: Index =
      index( "IDX_GENERATION_ACTIVITY", activityId, unique = false )

    // *
    def * : ProvenShape[GenerationEdge] =
      ( entityId, activityId ) <> ( ( GenerationEdge.apply _ ).tupled, GenerationEdge.unapply )
  }

  object generations extends TableQuery( new Generations( _ ) ) {
    val findByEntity = this.findBy( _.entityId )
    val findByActivity = this.findBy( _.activityId )
  }

  _schemas += generations.schema
}
