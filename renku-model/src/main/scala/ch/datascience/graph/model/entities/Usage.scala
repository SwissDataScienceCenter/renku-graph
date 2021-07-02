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

package ch.datascience.graph.model.entities

import ch.datascience.graph.model.Schemas.prov
import ch.datascience.graph.model.usages.ResourceId
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

final case class Usage(resourceId: ResourceId, entity: Entity)

object Usage {
  implicit lazy val encoder: JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { case Usage(resourceId, entity) =>
      JsonLD.entity(
        resourceId.asEntityId,
        EntityTypes of (prov / "Usage"),
        prov / "entity" -> entity.asJsonLD
      )
    }
}
