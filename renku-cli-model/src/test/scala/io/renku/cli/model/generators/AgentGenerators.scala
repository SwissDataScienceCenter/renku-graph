/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import io.renku.cli.model.CliAgent
import io.renku.cli.model.generators.PersonGenerators.cliPersonGen
import io.renku.cli.model.generators.SoftwareAgentGenerators.softwareAgentGen
import io.renku.graph.model.RenkuUrl
import org.scalacheck.Gen

trait AgentGenerators {

  val agentSoftwareGenerator:                            Gen[CliAgent.Software] = softwareAgentGen.map(CliAgent.apply)
  def agentPersonGenerator(implicit renkuUrl: RenkuUrl): Gen[CliAgent.Person]   = cliPersonGen.map(CliAgent.apply)

  def agentGenerator(implicit renkuUrl: RenkuUrl): Gen[CliAgent] =
    Gen.oneOf(agentSoftwareGenerator, agentPersonGenerator)
}

object AgentGenerators extends AgentGenerators
