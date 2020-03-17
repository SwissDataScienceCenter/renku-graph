package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.ResourceId
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder, entities}
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGInfoFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "findProject" should {

    "return details of the project from the KG if it exists" in new TestCase {
      forAll { entitiesProject: entities.Project =>
        loadToStore(entitiesProject.asJsonLD)

        val Some(kgProject) = finder.findProject(entitiesProject.path).unsafeRunSync()

        kgProject.resourceId shouldBe ResourceId(renkuBaseUrl, entitiesProject.path)
        kgProject.maybeParentResourceId shouldBe entitiesProject.maybeParentProject.map(
          parent => ResourceId(renkuBaseUrl, parent.path)
        )
        kgProject.dateCreated        shouldBe entitiesProject.dateCreated
        kgProject.creator.maybeEmail shouldBe entitiesProject.creator.maybeEmail
        kgProject.creator.maybeName  shouldBe entitiesProject.creator.name.some
      }
    }

    "return no details if the project does not exists in the KG" in new TestCase {
      finder
        .findProject(projectPaths.generateOne)
        .unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val finder               = new IOKGInfoFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }

  private implicit val projectsGen: Gen[entities.Project] = for {
    maybeParent <- entitiesProjects(maybeParentProject = None).toGeneratorOfOptions
    project     <- entitiesProjects(maybeParentProject = maybeParent)
  } yield project
}
