package io.renku.eventlog.events.categories.cleanuprequest

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CleanUpRequestEventSpec extends AnyWordSpec with should.Matchers {

  "Full.show" should {
    "return String representation of the underlying project id and path" in {
      val id   = projectIds.generateOne
      val path = projectPaths.generateOne

      CleanUpRequestEvent(id, path).show shouldBe show"projectId = $id, projectPath = $path"
    }
  }

  "Partial.show" should {
    "return String representation of the underlying project path" in {
      val path = projectPaths.generateOne

      CleanUpRequestEvent(path).show shouldBe show"projectPath = $path"
    }
  }
}
