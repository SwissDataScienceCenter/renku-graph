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

package io.renku.cli.model

import Ontologies.{Prov, Schema}
import io.renku.graph.model.agents._
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliSoftwareAgent(
    resourceId: ResourceId,
    name:       Name
) extends CliModel

object CliSoftwareAgent {
  private val entityTypes: EntityTypes = EntityTypes.of(Prov.SoftwareAgent)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit val jsonLDEncoder: JsonLDEncoder[CliSoftwareAgent] =
    JsonLDEncoder.instance { agent =>
      JsonLD.entity(
        agent.resourceId.asEntityId,
        entityTypes,
        Schema.name -> agent.name.asJsonLD
      )
    }

  implicit val jsonLDDecoder: JsonLDEntityDecoder[CliSoftwareAgent] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        label      <- cursor.downField(Schema.name).as[Name]
      } yield CliSoftwareAgent(resourceId, label)
    }
}
