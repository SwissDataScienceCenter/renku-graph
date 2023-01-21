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

import io.renku.cli.model._
import io.renku.generators.Generators
import io.renku.graph.model._
import org.scalacheck.Gen

import java.time.Instant

trait PlanGenerators {

  def planGen(minCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliPlan] =
    for {
      id               <- RenkuTinyTypeGenerators.planResourceIds
      name             <- RenkuTinyTypeGenerators.planNames
      descr            <- Gen.option(RenkuTinyTypeGenerators.planDescriptions)
      creators         <- Generators.listOf(PersonGenerators.cliPersonGen, max = 3)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      dateModified     <- Gen.option(BaseGenerators.dateModified)
      keywords         <- Generators.listOf(RenkuTinyTypeGenerators.planKeywords, max = 3)
      command          <- Gen.option(RenkuTinyTypeGenerators.planCommands)
      parameters       <- Generators.listOf(CommandParameterGenerators.commandParameterGen, max = 3)
      inputs           <- Generators.listOf(CommandParameterGenerators.commandInputGen, max = 3)
      outputs          <- Generators.listOf(CommandParameterGenerators.commandOutputGen, max = 3)
      successCodes     <- Generators.listOf(RenkuTinyTypeGenerators.planSuccessCodes, max = 3)
      derivedFrom      <- Gen.option(RenkuTinyTypeGenerators.planDerivedFroms)
      invalidationTime <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(minCreated.minusMillis(1000)))
    } yield CliPlan(
      id,
      name,
      descr,
      creators,
      dateCreated,
      dateModified,
      keywords,
      command,
      parameters,
      inputs,
      outputs,
      successCodes,
      derivedFrom,
      invalidationTime
    )

  def compositePlanChildPlanGen(minCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliCompositePlan.ChildPlan] = {
    val plan = planGen(minCreated).map(CliCompositePlan.ChildPlan.apply)
    val cp   = compositePlanGen(minCreated).map(CliCompositePlan.ChildPlan.apply)
    Gen.frequency(1 -> cp, 9 -> plan)
  }

  def compositePlanGen(minCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliCompositePlan] =
    for {
      id               <- RenkuTinyTypeGenerators.planResourceIds
      name             <- RenkuTinyTypeGenerators.planNames
      description      <- Gen.option(RenkuTinyTypeGenerators.planDescriptions)
      creators         <- Generators.listOf(PersonGenerators.cliPersonGen, max = 3)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      keywords         <- Generators.listOf(RenkuTinyTypeGenerators.planKeywords, max = 3)
      derivedFrom      <- Gen.option(RenkuTinyTypeGenerators.planDerivedFroms)
      invalidationTime <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(minCreated.minusMillis(1000)))
      childPlans       <- Generators.nonEmptyList(compositePlanChildPlanGen(minCreated))
      links            <- Generators.listOf(CommandParameterGenerators.parameterLinkGen, max = 3)
      mappings         <- Generators.listOf(CommandParameterGenerators.parameterMappingGen, max = 3)
    } yield CliCompositePlan(
      id,
      name,
      description,
      creators,
      dateCreated,
      keywords,
      derivedFrom,
      invalidationTime,
      childPlans,
      links,
      mappings
    )

  def workflowFilePlanGen(minCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliWorkflowFilePlan] =
    for {
      id               <- RenkuTinyTypeGenerators.planResourceIds
      name             <- RenkuTinyTypeGenerators.planNames
      descr            <- Gen.option(RenkuTinyTypeGenerators.planDescriptions)
      creators         <- Generators.listOf(PersonGenerators.cliPersonGen, max = 3)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      dateModified     <- Gen.option(BaseGenerators.dateModified)
      keywords         <- Generators.listOf(RenkuTinyTypeGenerators.planKeywords, max = 3)
      command          <- Gen.option(RenkuTinyTypeGenerators.planCommands)
      parameters       <- Generators.listOf(CommandParameterGenerators.commandParameterGen, max = 3)
      inputs           <- Generators.listOf(CommandParameterGenerators.commandInputGen, max = 3)
      outputs          <- Generators.listOf(CommandParameterGenerators.commandOutputGen, max = 3)
      successCodes     <- Generators.listOf(RenkuTinyTypeGenerators.planSuccessCodes, max = 3)
      derivedFrom      <- Gen.option(RenkuTinyTypeGenerators.planDerivedFroms)
      invalidationTime <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(minCreated.minusMillis(1000)))
    } yield CliWorkflowFilePlan(
      id,
      name,
      descr,
      creators,
      dateCreated,
      dateModified,
      keywords,
      command,
      parameters,
      inputs,
      outputs,
      successCodes,
      derivedFrom,
      invalidationTime
    )

  def workflowFileCompositePlanGen(
      minCreated: Instant
  )(implicit renkuUrl: RenkuUrl): Gen[CliWorkflowFileCompositePlan] =
    for {
      id               <- RenkuTinyTypeGenerators.planResourceIds
      name             <- RenkuTinyTypeGenerators.planNames
      description      <- Gen.option(RenkuTinyTypeGenerators.planDescriptions)
      creators         <- Generators.listOf(PersonGenerators.cliPersonGen, max = 3)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      keywords         <- Generators.listOf(RenkuTinyTypeGenerators.planKeywords, max = 3)
      derivedFrom      <- Gen.option(RenkuTinyTypeGenerators.planDerivedFroms)
      invalidationTime <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(minCreated.minusMillis(1000)))
      childPlans       <- Generators.nonEmptyList(workflowFilePlanGen(minCreated), max = 3)
      links            <- Generators.listOf(CommandParameterGenerators.parameterLinkGen, max = 3)
      mappings         <- Generators.listOf(CommandParameterGenerators.parameterMappingGen, max = 3)
      path             <- Generators.relativePaths().map(entityModel.Location.FileOrFolder.apply) // TODO
    } yield CliWorkflowFileCompositePlan(
      id,
      name,
      description,
      creators,
      dateCreated,
      keywords,
      derivedFrom,
      invalidationTime,
      childPlans,
      links,
      mappings,
      path
    )
}

object PlanGenerators extends PlanGenerators
