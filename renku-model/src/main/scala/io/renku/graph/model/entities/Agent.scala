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

package io.renku.graph.model.entities

import cats.data.ValidatedNel
import cats.syntax.all._
import io.renku.cli.model.CliSoftwareAgent
import io.renku.graph.model.Schemas.{prov, schema}
import io.renku.graph.model.agents._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.jsonld.ontology._

final case class Agent(resourceId: ResourceId, name: Name)

object Agent {

  val entityTypes: EntityTypes = EntityTypes.of(prov / "SoftwareAgent")

  implicit lazy val encoder: JsonLDEncoder[Agent] = JsonLDEncoder.instance { agent =>
    import io.renku.graph.model.Schemas._
    import io.renku.jsonld.syntax._

    JsonLD.entity(
      agent.resourceId.asEntityId,
      entityTypes,
      schema / "name" -> agent.name.asJsonLD
    )
  }

  def fromCli(cliAgent: CliSoftwareAgent): ValidatedNel[String, Agent] =
    Agent(cliAgent.resourceId, cliAgent.name).validNel

  lazy val ontology: Type = Type.Def(
    Class(prov / "SoftwareAgent"),
    DataProperty(schema / "name", xsd / "string")
  )
}
