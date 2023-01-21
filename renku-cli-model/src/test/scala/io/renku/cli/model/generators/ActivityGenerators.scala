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
      usages      <- Generators.listOf(UsageGenerators.usageGen, max = 3)
      generations <- Generators.listOf(GenerationGenerators.generationGen(id), max = 3)
      parameters  <- Generators.listOf(ParameterValueGenerators.parameterValueGen, max = 3)
    } yield CliActivity(id, startTime, endTime, agent, association, usages, generations, parameters)
}

object ActivityGenerators extends ActivityGenerators
