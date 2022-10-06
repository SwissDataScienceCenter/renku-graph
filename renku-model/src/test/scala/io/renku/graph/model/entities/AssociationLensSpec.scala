package io.renku.graph.model.entities

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.associations.ResourceId
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class AssociationLensSpec extends AnyWordSpec with should.Matchers with EntitiesGenerators {

  "associationPlan" should {
    "get and set" in {
      val assoc1 = createAssociationPerson
      val assoc2 = createAssociationAgent

      AssociationLens.associationPlan.get(assoc1)              shouldBe assoc1.plan
      AssociationLens.associationPlan.get(assoc2)              shouldBe assoc2.plan
      AssociationLens.associationPlan.set(assoc1.plan)(assoc2) shouldBe assoc2.copy(plan = assoc1.plan)
      AssociationLens.associationPlan.set(assoc2.plan)(assoc1) shouldBe assoc1.copy(plan = assoc2.plan)
    }
  }

  "associationAgent" should {
    "get and set" in {
      val assoc1 = createAssociationAgent
      val assoc2 = createAssociationPerson

      AssociationLens.associationAgent.get(assoc1) shouldBe Left(assoc1.agent)
      AssociationLens.associationAgent.get(assoc2) shouldBe Right(assoc2.agent)

      AssociationLens.associationAgent.set(assoc1.agent.asLeft)(assoc2) shouldBe
        Association.WithRenkuAgent(assoc2.resourceId, assoc1.agent, assoc2.plan)
      AssociationLens.associationAgent.set(assoc2.agent.asRight)(assoc1) shouldBe
        Association.WithPersonAgent(assoc1.resourceId, assoc2.agent, assoc1.plan)

      val assoc3 = createAssociationAgent
      val assoc4 = createAssociationPerson
      AssociationLens.associationAgent.set(assoc1.agent.asLeft)(assoc3) shouldBe
        assoc3.copy(agent = assoc1.agent)
      AssociationLens.associationAgent.set(assoc2.agent.asRight)(assoc4) shouldBe
        assoc4.copy(agent = assoc2.agent)
    }
  }

  private def createPlan =
    planEntities()
      .apply(GraphModelGenerators.projectCreatedDates().generateOne)
      .generateOne
      .to[Plan]

  private def createAssociationAgent: Association.WithRenkuAgent =
    Association.WithRenkuAgent(
      ResourceId(s"http://localhost/${activityIds.generateOne.value}"),
      EntitiesGenerators.agentEntities.generateOne,
      createPlan
    )

  private def createAssociationPerson: Association.WithPersonAgent =
    Association.WithPersonAgent(
      ResourceId(s"http://localhost/${activityIds.generateOne.value}"),
      personEntities.generateOne.to[Person],
      createPlan
    )
}
