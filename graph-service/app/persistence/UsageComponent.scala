package persistence

import models.{ Activity, Entity, UsageEdge }
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.{ ForeignKeyQuery, Index, ProvenShape }

import scala.concurrent.Future

trait UsageComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ExecutionContextComponent with ActivityComponent with EntityComponent =>

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
    val findByActivity = Compiled { activityId: Rep[String] =>
      for {
        edge <- this.findBy( _.activityId ).extract.apply( activityId )
        entity <- edge.entity
      } yield ( edge, entity )
    }

    val findByEntity = Compiled { entityId: Rep[String] =>
      for {
        edge <- this.findBy( _.entityId ).extract.apply( entityId )
        activity <- edge.activity
      } yield ( edge, activity )
    }

    object lowLevelApi {
      def findByActivity( activityId: String ): DBIO[Seq[( UsageEdge, Entity )]] = {
        usages.findByActivity( activityId ).result
      }

      def findByEntity( entityId: String ): DBIO[Seq[( UsageEdge, Activity )]] = {
        usages.findByEntity( entityId ).result
      }
    }

    object api {
      def findByActivity( activityIds: Seq[String] ): Future[Seq[( UsageEdge, Entity )]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { activityId <- activityIds } yield lowLevelApi.findByActivity( activityId ) )
        } yield seq.flatten
        db.run( dbio )
      }

      def findByEntity( entityIds: Seq[String] ): Future[Seq[( UsageEdge, Activity )]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { entityId <- entityIds } yield lowLevelApi.findByEntity( entityId ) )
        } yield seq.flatten
        db.run( dbio )
      }
    }
  }

  _schemas += usages.schema
}
