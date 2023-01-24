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

import io.renku.cli.model.Ontologies.Prov
import io.renku.graph.model.usages._
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliUsage(resourceId: ResourceId, entity: CliSingleEntity) extends CliModel

object CliUsage {

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Usage)

  implicit lazy val decoder: JsonLDDecoder[CliUsage] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        entity     <- cursor.downField(Prov.entity).as[CliSingleEntity]
      } yield CliUsage(resourceId, entity)
    }

  implicit lazy val jsonLDEncoder: JsonLDEncoder[CliUsage] =
    JsonLDEncoder.instance { usage =>
      JsonLD.entity(
        usage.resourceId.asEntityId,
        entityTypes,
        Prov.entity -> usage.entity.asJsonLD
      )
    }
}
