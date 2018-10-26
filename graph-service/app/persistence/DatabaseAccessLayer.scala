package persistence

import akka.Done
import javax.inject.{ Inject, Singleton }
import models.{ Activity, AssociationEdge, Entity, GenerationEdge, Person, Project, UsageEdge }
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
    val activitiesObjects = Utils.loadJsonData[Activity]( "/site-data/tutorial-zhbikes/activities.json" ).get
    val addActivities = ( this.activities ++= activitiesObjects )

    val entitiesObjects = Utils.loadJsonData[Entity]( "/site-data/tutorial-zhbikes/entities.json" ).get
    val addEntities = ( this.entities ++= entitiesObjects )

    val personsObjects = Utils.loadJsonData[Person]( "/site-data/tutorial-zhbikes/persons.json" ).get
    val addPersons = ( this.persons ++= personsObjects )

    val projectsObjects = Utils.loadJsonData[Project]( "/site-data/tutorial-zhbikes/projects.json" ).get
    val addProjects = ( this.projects ++= projectsObjects )

    val associationsObjects = Utils.loadJsonData[AssociationEdge]( "/site-data/tutorial-zhbikes/associations.json" ).get
    val addAssociations = ( this.associations ++= associationsObjects )

    val generationsObjects = Utils.loadJsonData[GenerationEdge]( "/site-data/tutorial-zhbikes/generations.json" ).get
    val addGenerations = ( this.generations ++= generationsObjects )

    val usagesObjects = Utils.loadJsonData[UsageEdge]( "/site-data/tutorial-zhbikes/usages.json" ).get
    val addUsages = ( this.usages ++= usagesObjects )

    for {
      _ <- db.run(
        DBIO.sequence( List(
          addActivities,
          addEntities,
          addPersons,
          addProjects,
          addAssociations,
          addGenerations,
          addUsages
        ) )
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
