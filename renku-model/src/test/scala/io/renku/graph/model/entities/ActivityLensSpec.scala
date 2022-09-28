package io.renku.graph.model.entities

import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.projects.DateCreated
import io.renku.graph.model.testentities.generators.{ActivityGenerators, EntitiesGenerators}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ActivityLensSpec extends AnyWordSpec with should.Matchers {
  implicit val renkuUrl: RenkuUrl = EntitiesGenerators.renkuUrl
  import io.renku.generators.Generators.Implicits._

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
    ActivityGenerators
      .activityEntities(ActivityGenerators.planEntities()(ActivityGenerators.planCommands))
      .apply(DateCreated(Instant.EPOCH))
      .generateOne
      .to[Activity]
}
