package io.renku.entities.viewings.search

import cats.effect.IO
import cats.syntax.all._
import io.renku.entities.search.FinderSpecOps
import io.renku.entities.searchgraphs.SearchInfoDataset
import io.renku.entities.viewings.collector.datasets.EventUploader
import io.renku.entities.viewings.collector.projects.viewed.EventPersister
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{datasets, entities, projects}
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent, UserId}
import io.renku.triplesstore.{ExternalJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

class RecentEntitiesFinderSpec
    extends AnyFlatSpec
    with should.Matchers
    with EntitiesGenerators
    with FinderSpecOps
    with ExternalJenaForSpec
    with ProjectsDataset
    with SearchInfoDataset
    with IOSpec {

  implicit override def ioLogger: Logger[IO] = Slf4jLogger.getLogger[IO]

  implicit val queryTimeRecorder: SparqlQueryTimeRecorder[IO] =
    TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

  lazy val eventUploader: EventUploader[IO] =
    EventUploader[IO](projectsDSConnectionInfo)
      .unsafeRunSync()

  lazy val eventPersister: EventPersister[IO] =
    EventPersister[IO](projectsDSConnectionInfo).unsafeRunSync()

  def storeEvent1(userId: GitLabId, dateViewed: Instant, path: projects.Path): Unit =
    eventPersister.persist(ProjectViewedEvent(path, dateViewed, UserId.GLId(userId).some)).unsafeRunSync()

  def storeEvent2(userId: GitLabId, dateViewed: Instant, ident: datasets.Identifier): Unit =
    eventUploader.upload(DatasetViewedEvent(ident, dateViewed, userId.some)).unsafeRunSync()

  it should "find things" in {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val entitiesProject = project.to[entities.RenkuProject.WithoutParent]
    val user            = entitiesProject.maybeCreator.get.maybeGitLabId.get

    provisionTestProjects(project).unsafeRunSync()
    storeEvent2(user, Instant.now(), entitiesProject.datasets.head.identification.identifier)
    storeEvent1(user, Instant.now(), entitiesProject.path)

  }

}
