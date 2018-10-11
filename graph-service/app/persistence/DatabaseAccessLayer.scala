package persistence

import akka.Done
import graphql.Utils
import javax.inject.{ Inject, Singleton }
import models.{ Activity, Entity, GenerationEdge }
import play.api.Logger
import play.api.db.slick.{ DatabaseConfigProvider, HasDatabaseConfig }
import slick.basic.{ BasicProfile, DatabaseConfig }
import slick.jdbc.JdbcProfile

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

@Singleton
class DatabaseAccessLayer @Inject() (
    protected val dbConfigProvider: DatabaseConfigProvider,
    implicit val ec:                ExecutionContext
) extends DatabaseStack( dbConfig = dbConfigProvider.get )
  with HasDatabaseConfig[JdbcProfile] { that =>
  import profile.api._

  lazy val logger: Logger = Logger(
    "application.persistence.DatabaseAccessLayer"
  )

  object lowLevel {
    def allActivities: DBIO[Seq[Activity]] = {
      that.activities.result
    }

    def findActivity( id: String ): DBIO[Seq[Activity]] = {
      that.activities.findById( id ).result
    }

    def findActivitiesGenerating( entityId: String ): DBIO[Seq[Activity]] = {
      ( for {
        edges <- that.generations.findByEntity( entityId ).extract
        activity <- edges.activity
      } yield activity ).result
    }

    def allEntities: DBIO[Seq[Entity]] = {
      that.entities.result
    }

    def findEntity( id: String ): DBIO[Seq[Entity]] = {
      that.entities.findById( id ).result
    }

    def findEntitiesGeneratedBy( activityId: String ): DBIO[Seq[Entity]] = {
      ( for {
        edges <- that.generations.findByActivity( activityId ).extract
        entity <- edges.entity
      } yield entity ).result
    }

    def findGenerationEdgesByActivity( activityId: String ): DBIO[Seq[GenerationEdge]] = {
      that.generations.findByActivity( activityId ).result
    }

    def findGenerationEdgesByEntity( entityId: String ): DBIO[Seq[GenerationEdge]] = {
      that.generations.findByEntity( entityId ).result
    }
  }

  object highLevel {
    def activities: Future[Seq[Activity]] = {
      db.run( lowLevel.allActivities )
    }

    def activities( ids: Seq[String] ): Future[Seq[Activity]] = {
      val dbio = for {
        seq <- DBIO.sequence( for { id <- ids } yield lowLevel.findActivity( id ) )
      } yield seq.flatten
      db.run( dbio )
    }

    def activitiesGenerating( entityIds: Seq[String] ): Future[Seq[Activity]] = {
      val dbio = for {
        seq <- DBIO.sequence(
          for { id <- entityIds } yield lowLevel.findActivitiesGenerating( id )
        )
      } yield seq.flatten
      db.run( dbio )
    }

    def entities: Future[Seq[Entity]] = {
      db.run( lowLevel.allEntities )
    }

    def entities( ids: Seq[String] ): Future[Seq[Entity]] = {
      val dbio = for {
        seq <- DBIO.sequence( for { id <- ids } yield lowLevel.findEntity( id ) )
      } yield seq.flatten
      db.run( dbio )
    }

    def entitiesGeneratedBy( activityIds: Seq[String] ): Future[Seq[Entity]] = {
      val dbio = for {
        seq <- DBIO.sequence(
          for { id <- activityIds } yield lowLevel.findEntitiesGeneratedBy( id )
        )
      } yield seq.flatten
      db.run( dbio )
    }

    def generationEdgesByActivities( activityIds: Seq[String] ): Future[Seq[GenerationEdge]] = {
      val dbio = for {
        seq <- DBIO.sequence( for { id <- activityIds } yield lowLevel.findGenerationEdgesByActivity( id ) )
      } yield seq.flatten
      db.run( dbio )
    }

    def generationEdgesByEntities( entityId: Seq[String] ): Future[Seq[GenerationEdge]] = {
      val dbio = for {
        seq <- DBIO.sequence( for { id <- entityId } yield lowLevel.findGenerationEdgesByEntity( id ) )
      } yield seq.flatten
      db.run( dbio )
    }
  }

  def init: Future[Done] = _init

  protected val _init: Future[Done] = {
    for {
      _ <- initTables()
      _ <- initData()
    } yield Done
  }

  protected def initTables(): Future[Seq[Throwable]] = {
    val createFutures = for {
      schema <- this.schemas
    } yield db.run( schema.create ).transform( x => Try( x.toEither ) )

    for {
      seq <- Future.sequence( createFutures )
    } yield for {
      either <- seq
      if either.isLeft
    } yield either.left.get
  }

  protected def initData(): Future[Done] = {
    val activitiesObjects = Utils
      .loadJsonData[Activity]( "/site-data/tutorial-zhbikes/activities.json" )
      .get
    val addActivities = ( this.activities ++= activitiesObjects )

    val entitiesObjects =
      Utils
        .loadJsonData[Entity]( "/site-data/tutorial-zhbikes/entities.json" )
        .get
    val addEntities = ( this.entities ++= entitiesObjects )

    val generationsObjects = Utils
      .loadJsonData[GenerationEdge](
        "/site-data/tutorial-zhbikes/generations.json"
      )
      .get
    val addGenerations = ( this.generations ++= generationsObjects )

    for {
      _ <- db.run(
        DBIO.sequence( List( addActivities, addEntities, addGenerations ) )
      )
    } yield Done
  }
}

object DatabaseAccessLayer {
  def apply( dbConfig: DatabaseConfig[JdbcProfile] )(
      implicit
      ec: ExecutionContext
  ): DatabaseAccessLayer =
    new DatabaseAccessLayer( new DatabaseConfigProvider {
      override def get[P <: BasicProfile]: DatabaseConfig[P] =
        dbConfig.asInstanceOf[DatabaseConfig[P]]
    }, ec )
}
