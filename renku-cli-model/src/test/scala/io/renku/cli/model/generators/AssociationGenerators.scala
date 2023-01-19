package io.renku.cli.model.generators

import io.renku.cli.model.CliAssociation
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalacheck.Gen

import java.time.Instant

trait AssociationGenerators {

  def associatedPlanGen(planMinCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliAssociation.AssociatedPlan] =
    Gen.oneOf(
      PlanGenerators.planGen(planMinCreated).map(CliAssociation.AssociatedPlan.apply),
      PlanGenerators.workflowFilePlanGen(planMinCreated).map(CliAssociation.AssociatedPlan.apply),
      PlanGenerators.workflowFileCompositePlanGen(planMinCreated).map(CliAssociation.AssociatedPlan.apply)
    )

  def associationGen(planMinCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliAssociation] =
    for {
      id    <- RenkuTinyTypeGenerators.associationResourceIdGen
      agent <- Gen.option(AgentGenerators.agentGen)
      plan  <- associatedPlanGen(planMinCreated)
    } yield CliAssociation(id, agent, plan)
}

object AssociationGenerators extends AssociationGenerators
