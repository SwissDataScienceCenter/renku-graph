package io.renku.knowledgegraph.entities

import cats.effect.IO
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import io.renku.graph.model.testentities._
import io.renku.generators.Generators.Implicits._

class EntitiesFinderSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore with IOSpec {

  "findEntities" should {

    "return all entities sorted by name if no query is given" in new TestCase {
      val project = projectEntities(visibilityPublic)
        .withDatasets(datasetEntities(provenanceNonModified))
        .withActivities(activityEntities(planEntities()))
        .generateOne

      loadToStore(project)

      finder.findEntities().unsafeRunSync() shouldBe List(
        project.to[model.Project]
      )
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())
    val finder               = new EntitiesFinderImpl[IO](rdfStoreConfig, timeRecorder)
  }
}
