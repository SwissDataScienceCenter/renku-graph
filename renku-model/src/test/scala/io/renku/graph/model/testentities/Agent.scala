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

package io.renku.graph.model.testentities

import cats.syntax.all._
import io.renku.cli.model.CliSoftwareAgent
import io.renku.graph.model.agents.Name
import io.renku.graph.model.cli.CliConverters
import io.renku.graph.model.versions.CliVersion
import io.renku.graph.model.{agents, entities}

final case class Agent(cliVersion: CliVersion)

object Agent {

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit lazy val toEntitiesAgent: Agent => entities.Agent = agent =>
    entities.Agent(agents.ResourceId(agent.asEntityId.show), Name(s"renku ${agent.cliVersion}"))

  implicit def toCliAgent: Agent => CliSoftwareAgent =
    CliConverters.from(_)

  implicit lazy val encoder: JsonLDEncoder[Agent] = JsonLDEncoder.instance {
    _.to[entities.Agent].asJsonLD
  }

  implicit lazy val entityIdEncoder: EntityIdEncoder[Agent] =
    EntityIdEncoder.instance(entity =>
      EntityId of s"https://github.com/swissdatasciencecenter/renku-python/tree/v${entity.cliVersion}"
    )
}
