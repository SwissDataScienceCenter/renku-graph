package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.renkuBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.rdfstore.entities.ProjectsGenerators._
import ch.datascience.rdfstore.entities.bundles.generateAgent
import io.circe.Json
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class ProjectPathExtractorSpec extends AnyWordSpec with should.Matchers {

  "extractProjectPath" should {

    "find project path in the JSON-LD payload if there's a single Project object" in new TestCase {

      val projectPath   = projectPaths.generateOne
      val projectJsonLd = projects.generateOne.copy(path = projectPath).asJsonLD

      extractor.extractProjectPath(
        JsonLDTriples(Json.arr(projectJsonLd.toJson, generateAgent.asJsonLD.toJson))
      ) shouldBe projectPath.pure[Try]
    }

    "fail if no project path can be found in the JSON-LD payload" in new TestCase {
      val Failure(exception) = extractor.extractProjectPath(JsonLDTriples(Json.Null))

      exception.getMessage shouldBe "No project found in the payload"
    }

    "fail if there's more than one Project object in the JSON-LD payload" in new TestCase {
      val Failure(exception) =
        extractor.extractProjectPath(
          JsonLDTriples(Json.arr(projects.generateOne.asJsonLD.toJson, projects.generateOne.asJsonLD.toJson))
        )

      exception.getMessage shouldBe "More than project found in the payload"
    }
  }

  private trait TestCase {
    implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne

    val extractor = new ProjectPathExtractorImpl[Try]()
  }
}
