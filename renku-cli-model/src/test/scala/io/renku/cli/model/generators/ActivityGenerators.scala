package io.renku.cli.model.generators

import io.renku.cli.model.CliActivity
import io.renku.generators.Generators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalacheck.Gen

import java.time.Instant

trait ActivityGenerators {

  def activityAgentGen(implicit renkuUrl: RenkuUrl): Gen[CliActivity.Agent] =
    Gen.oneOf(
      PersonGenerators.cliPersonGen.map(CliActivity.Agent.apply),
      AgentGenerators.agentGen.map(CliActivity.Agent.apply)
    )

  def activityGen(planMinCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliActivity] =
    for {
      id          <- RenkuTinyTypeGenerators.activityResourceIdGen
      startTime   <- RenkuTinyTypeGenerators.activityStartTimes
      endTime     <- RenkuTinyTypeGenerators.activityEndTimeGen
      agent       <- activityAgentGen
      association <- AssociationGenerators.associationGen(planMinCreated)
      usages      <- Generators.listOf(UsageGenerators.usageGen)
      generations <- Generators.listOf(GenerationGenerators.generationGen)
      parameters  <- Generators.listOf(ParameterValueGenerators.parameterValueGen)
    } yield CliActivity(id, startTime, endTime, agent, association, usages, generations, parameters)

}

object ActivityGenerators extends ActivityGenerators
