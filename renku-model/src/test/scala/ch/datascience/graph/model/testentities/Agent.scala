/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model.testentities

import cats.syntax.all._
import ch.datascience.graph.model.agents.Label
import ch.datascience.graph.model.{CliVersion, agents, entities}

final case class Agent(cliVersion: CliVersion)

object Agent {

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit lazy val toEntitiesAgent: Agent => entities.Agent = agent =>
    entities.Agent(agents.ResourceId(agent.asEntityId.show), Label(s"renku ${agent.cliVersion}"))

  implicit lazy val encoder: JsonLDEncoder[Agent] = JsonLDEncoder.instance {
    _.to[entities.Agent].asJsonLD
  }

  implicit lazy val entityIdEncoder: EntityIdEncoder[Agent] =
    EntityIdEncoder.instance(entity =>
      EntityId of s"https://github.com/swissdatasciencecenter/renku-python/tree/v${entity.cliVersion}"
    )
}
