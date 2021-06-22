package io.renku.eventlog.events.categories.statuschange

import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus
import io.renku.eventlog.events.categories.statuschange.DBUpdateResults.ForProjects
import org.scalacheck.Gen
import cats.syntax.all._
import org.scalatest.matchers.should

class DBUpdateResultsSpec extends AnyWordSpec with should.Matchers {
  "combine" should {
    "merge status count for each project" in {
      val project1         = projectPaths.generateOne
      val project1Count    = EventStatus.all.map(_ -> Gen.choose(-200, 200).generateOne).toMap
      val project2         = projectPaths.generateOne
      val project2Count    = EventStatus.all.map(_ -> Gen.choose(-200, 200).generateOne).toMap
      val uniqueProject    = ForProjects(Set(project1 -> project1Count))
      val multipleProjects = ForProjects(Set(project1 -> project1Count, project2 -> project2Count))

      uniqueProject.combine(multipleProjects) shouldBe ForProjects(
        Set(project1 -> project1Count.combine(project1Count), project2 -> project2Count)
      )
    }
  }
}
