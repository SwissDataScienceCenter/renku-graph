package io.renku.cli.model.generators

import io.renku.cli.model.{CliCompositePlan, CliPlan, CliWorkflowFileCompositePlan, CliWorkflowFilePlan}
import io.renku.generators.Generators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl, entityModel, plans}
import org.scalacheck.Gen

import java.time.Instant

trait PlanGenerators {

  def planGen(minCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliPlan] =
    for {
      id               <- RenkuTinyTypeGenerators.planResourceIds
      name             <- RenkuTinyTypeGenerators.planNames
      descr            <- Gen.option(RenkuTinyTypeGenerators.planDescriptions)
      creators         <- Gen.listOf(PersonGenerators.cliPersonGen)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      dateModified     <- Gen.option(BaseGenerators.dateModified)
      keywords         <- Gen.listOf(RenkuTinyTypeGenerators.planKeywords)
      command          <- Gen.option(RenkuTinyTypeGenerators.planCommands)
      parameters       <- Gen.listOf(CommandParameterGenerators.commandParameterGen)
      inputs           <- Gen.listOf(CommandParameterGenerators.commandInputGen)
      outputs          <- Gen.listOf(CommandParameterGenerators.commandOutputGen)
      successCodes     <- Gen.listOf(RenkuTinyTypeGenerators.planSuccessCodes)
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
      creators         <- Gen.listOf(PersonGenerators.cliPersonGen)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      keywords         <- Gen.listOf(RenkuTinyTypeGenerators.planKeywords)
      derivedFrom      <- Gen.option(RenkuTinyTypeGenerators.planDerivedFroms)
      invalidationTime <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(minCreated.minusMillis(1000)))
      childPlans       <- Generators.nonEmptyList(compositePlanChildPlanGen(minCreated))
      links            <- Gen.listOf(CommandParameterGenerators.parameterLinkGen)
      mappings         <- Gen.listOf(CommandParameterGenerators.parameterMappingGen)
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
      creators         <- Gen.listOf(PersonGenerators.cliPersonGen)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      dateModified     <- Gen.option(BaseGenerators.dateModified)
      keywords         <- Gen.listOf(RenkuTinyTypeGenerators.planKeywords)
      command          <- Gen.option(RenkuTinyTypeGenerators.planCommands)
      parameters       <- Gen.listOf(CommandParameterGenerators.commandParameterGen)
      inputs           <- Gen.listOf(CommandParameterGenerators.commandInputGen)
      outputs          <- Gen.listOf(CommandParameterGenerators.commandOutputGen)
      successCodes     <- Gen.listOf(RenkuTinyTypeGenerators.planSuccessCodes)
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
      minCreated:      Instant
  )(implicit renkuUrl: RenkuUrl): Gen[CliWorkflowFileCompositePlan] =
    for {
      id               <- RenkuTinyTypeGenerators.planResourceIds
      name             <- RenkuTinyTypeGenerators.planNames
      description      <- Gen.option(RenkuTinyTypeGenerators.planDescriptions)
      creators         <- Gen.listOf(PersonGenerators.cliPersonGen)
      dateCreated      <- RenkuTinyTypeGenerators.planDatesCreated(plans.DateCreated(minCreated))
      keywords         <- Gen.listOf(RenkuTinyTypeGenerators.planKeywords)
      derivedFrom      <- Gen.option(RenkuTinyTypeGenerators.planDerivedFroms)
      invalidationTime <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(minCreated.minusMillis(1000)))
      childPlans       <- Generators.nonEmptyList(workflowFilePlanGen(minCreated))
      links            <- Gen.listOf(CommandParameterGenerators.parameterLinkGen)
      mappings         <- Gen.listOf(CommandParameterGenerators.parameterMappingGen)
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
