package persistence

import models.{ Activity, Entity, UsageEdge }
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.{ ForeignKeyQuery, Index, ProvenShape }

trait UsageComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ActivityComponent with EntityComponent =>

  import profile.api._

  class Usages( tag: Tag )
    extends Table[UsageEdge]( tag, "USAGE_EDGES" ) {
    // Columns
    def activityId: Rep[String] = column[String]( "ACTIVITY" )
    def entityId: Rep[String] = column[String]( "ENTITY" )

    // Foreign Keys
    def activity: ForeignKeyQuery[Activities, Activity] =
      foreignKey( "FK_USAGE_ACTIVITY", activityId, activities )(
        _.id,
        onUpdate = ForeignKeyAction.Cascade,
        onDelete = ForeignKeyAction.Restrict
      )
    def entity: ForeignKeyQuery[Entities, Entity] =
      foreignKey( "FK_USAGE_ENTITY", entityId, entities )(
        _.id,
        onUpdate = ForeignKeyAction.Cascade,
        onDelete = ForeignKeyAction.Restrict
      )

    // Indexes.
    def idxActivity: Index =
      index( "IDX_USAGE_ACTIVITY", activityId, unique = false )
    def idxEntity: Index =
      index( "IDX_USAGE_ENTITY", entityId, unique = false )

    // *
    def * : ProvenShape[UsageEdge] =
      ( activityId, entityId ) <> ( ( UsageEdge.apply _ ).tupled, UsageEdge.unapply )
  }

  object usages extends TableQuery( new Usages( _ ) ) {
    val findByActivity = this.findBy( _.activityId )
    val findByEntity = this.findBy( _.entityId )
  }

  _schemas += usages.schema
}
