package persistence

import models.{ Activity, Entity, GenerationEdge }
import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.{ ForeignKeyQuery, Index, ProvenShape }

import scala.concurrent.Future

trait GenerationComponent {
  this: HasDatabaseConfig[JdbcProfile] with SchemasComponent with ExecutionContextComponent with ActivityComponent with EntityComponent =>

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
    val findByEntity = Compiled { entityId: Rep[String] =>
      for {
        edge <- this.findBy( _.entityId ).extract.apply( entityId )
        activity <- edge.activity
      } yield ( edge, activity )
    }

    val findByActivity = Compiled { activityId: Rep[String] =>
      for {
        edge <- this.findBy( _.activityId ).extract.apply( activityId )
        entity <- edge.entity
      } yield ( edge, entity )
    }

    object lowLevelApi {
      def findByEntity( entityId: String ): DBIO[Seq[( GenerationEdge, Activity )]] = {
        generations.findByEntity( entityId ).result
      }

      def findByActivity( activityId: String ): DBIO[Seq[( GenerationEdge, Entity )]] = {
        generations.findByActivity( activityId ).result
      }
    }

    object api {
      def findByEntity( entityIds: Seq[String] ): Future[Seq[( GenerationEdge, Activity )]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { entityId <- entityIds } yield lowLevelApi.findByEntity( entityId ) )
        } yield seq.flatten
        db.run( dbio )
      }

      def findByActivity( activityIds: Seq[String] ): Future[Seq[( GenerationEdge, Entity )]] = {
        val dbio = for {
          seq <- DBIO.sequence( for { activityId <- activityIds } yield lowLevelApi.findByActivity( activityId ) )
        } yield seq.flatten
        db.run( dbio )
      }
    }
  }

  _schemas += generations.schema
}
