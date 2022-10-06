package io.renku.graph.model.entities

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ActivityLensSpec extends AnyWordSpec with should.Matchers with EntitiesGenerators {

  "activityAssociation" should {
    "get and set" in {
      val a1 = createActivity
      val a2 = createActivity
      ActivityLens.activityAssociation.get(a1)                 shouldBe a1.association
      ActivityLens.activityAssociation.set(a2.association)(a1) shouldBe a1.copy(association = a2.association)
    }
  }

  "activityAuthor" should {
    "get and set" in {
      val activity = createActivity
      val person   = createActivity.author
      ActivityLens.activityAuthor.get(activity)         shouldBe activity.author
      ActivityLens.activityAuthor.set(person)(activity) shouldBe activity.copy(author = person)
    }
  }

  private def createActivity =
    activityEntities(planEntities())
      .apply(GraphModelGenerators.projectCreatedDates().generateOne)
      .generateOne
      .to[Activity]
}