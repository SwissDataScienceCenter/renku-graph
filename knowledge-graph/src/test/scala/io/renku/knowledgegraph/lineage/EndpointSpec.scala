package io.renku.knowledgegraph.lineage

import cats.effect.IO
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.knowledgegraph.lineage.LineageGenerators._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec with should.Matchers with MockFactory with IOSpec {
  "GET /lineage" should {
    "respond with OK and the lineage" in new TestCase {
      val lineage = lineages.generateOne

      endpoint.`GET /lineage`(projectPath, location, maybeUser).unsafeRunSync() shouldBe lineage
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne
    val location    = nodeLocations.generateOne
    val maybeUser   = authUsers.generateOption

    val lineageFinder = mock[LineageFinder[IO]]
    val endpoint      = new EndpointImpl[IO](lineageFinder)
  }
}
