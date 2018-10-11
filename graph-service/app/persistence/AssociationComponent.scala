package persistence

import models.{ Activity, Entity, AssociationEdge, Person }
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.{ ForeignKeyQuery, Index, ProvenShape }

trait AssociationComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ActivityComponent with EntityComponent with PersonComponent =>

  import profile.api._

  class Associations( tag: Tag )
    extends Table[AssociationEdge]( tag, "ASSOCIATION_EDGES" ) {
    // Columns
    def activityId: Rep[String] = column[String]( "ACTIVITY" )
    def agentId: Rep[String] = column[String]( "AGENT" )
    def planId: Rep[String] = column[String]( "PLAN" )

    // Foreign Keys
    def activity: ForeignKeyQuery[Activities, Activity] =
      foreignKey( "FK_ASSOCIATION_ACTIVITY", activityId, activities )(
        _.id,
        onUpdate = ForeignKeyAction.Cascade,
        onDelete = ForeignKeyAction.Restrict
      )
    def agent: ForeignKeyQuery[Persons, Person] =
      foreignKey( "FK_ASSOCIATION_AGENT", agentId, persons )(
        _.id,
        onUpdate = ForeignKeyAction.Cascade,
        onDelete = ForeignKeyAction.Restrict
      )
    def plan: ForeignKeyQuery[Entities, Entity] =
      foreignKey( "FK_ASSOCIATION_PLAN", planId, entities )(
        _.id,
        onUpdate = ForeignKeyAction.Cascade,
        onDelete = ForeignKeyAction.Restrict
      )

    // Indexes.
    def idxActivity: Index =
      index( "IDX_ASSOCIATION_ACTIVITY", activityId, unique = false )
    def idxAgent: Index =
      index( "IDX_ASSOCIATION_AGENT", agentId, unique = false )

    // *
    def * : ProvenShape[AssociationEdge] =
      ( activityId, agentId, planId ) <> ( ( AssociationEdge.apply _ ).tupled, AssociationEdge.unapply )
  }

  object associations extends TableQuery( new Associations( _ ) ) {
    val findByActivity = this.findBy( _.activityId )
    val findByAgent = this.findBy( _.agentId )
  }

  _schemas += associations.schema
}
