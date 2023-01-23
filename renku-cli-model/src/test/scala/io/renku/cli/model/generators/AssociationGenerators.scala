/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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