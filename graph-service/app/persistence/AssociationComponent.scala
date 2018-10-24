package persistence

import models.{ Activity, AssociationEdge, Entity, Person }
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.{ ForeignKeyQuery, Index, ProvenShape }

import scala.concurrent.Future

trait AssociationComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ExecutionContextComponent with ActivityComponent with EntityComponent with PersonComponent =>

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
    val findByActivity = Compiled { activityId: Rep[String] =>
      for {
        edge <- this.findBy( _.activityId ).extract.apply( activityId )
        agent <- edge.agent
        plan <- edge.plan
      } yield ( edge, agent, plan )
    }

    val findByAgent = Compiled { agentId: Rep[String] =>
      for {
        edge <- this.findBy( _.agentId ).extract.apply( agentId )
        activity <- edge.activity
        plan <- edge.plan
      } yield ( edge, activity, plan )
    }

    object lowLevelApi {
      def findByActivity( activityId: String ): DBIO[Seq[( AssociationEdge, Person, Entity )]] = {
        associations.findByActivity( activityId ).result
      }

      def findByAgent( agentId: String ): DBIO[Seq[( AssociationEdge, Activity, Entity )]] = {
        associations.findByAgent( agentId ).result
      }
    }

    object api {
      def findByActivity( activityIds: Seq[String] ): Future[Seq[( AssociationEdge, Person, Entity )]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { activityId <- activityIds } yield lowLevelApi.findByActivity( activityId ) )
        } yield seq.flatten
        db.run( dbio )
      }

      def findByAgent( agentIds: Seq[String] ): Future[Seq[( AssociationEdge, Activity, Entity )]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { agentId <- agentIds } yield lowLevelApi.findByAgent( agentId ) )
        } yield seq.flatten
        db.run( dbio )
      }
    }
  }

  _schemas += associations.schema
}
