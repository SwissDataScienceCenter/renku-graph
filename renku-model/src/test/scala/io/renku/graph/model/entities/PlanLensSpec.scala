package io.renku.graph.model.entities

import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.projects.DateCreated
import io.renku.graph.model.testentities.generators.{ActivityGenerators, EntitiesGenerators}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class PlanLensSpec extends AnyWordSpec with should.Matchers {
  implicit val renkuUrl: RenkuUrl = EntitiesGenerators.renkuUrl
  import io.renku.generators.Generators.Implicits._

  "planCreators" should {
    "get and set" in {
      val p1 = createPlan
      val p2 = createPlan

      PlanLens.planCreators.get(p1)              shouldBe p1.creators
      PlanLens.planCreators.set(p2.creators)(p1) shouldBe p1.copy(creators = p2.creators)
    }
  }

  private def createPlan =
    EntitiesGenerators
      .planEntities()(ActivityGenerators.planCommands)
      .apply(DateCreated(Instant.EPOCH))
      .generateOne
      .to[Plan]
}
