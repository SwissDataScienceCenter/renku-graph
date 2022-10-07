package io.renku.graph.model.entities

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PlanLensSpec extends AnyWordSpec with should.Matchers with EntitiesGenerators {

  "planCreators" should {
    "get and set" in {
      val p1 = createPlan
      val p2 = createPlan

      PlanLens.planCreators.get(p1)              shouldBe p1.creators
      PlanLens.planCreators.set(p2.creators)(p1) shouldBe p1.copy(creators = p2.creators)
    }
  }

  private def createPlan =
    planEntities()
      .apply(GraphModelGenerators.projectCreatedDates().generateOne)
      .generateOne
      .to[Plan]
}
