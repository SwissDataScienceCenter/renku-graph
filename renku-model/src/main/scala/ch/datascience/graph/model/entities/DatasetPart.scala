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

import ch.datascience.graph.model.InvalidationTime
import ch.datascience.graph.model.datasets.{DateCreated, PartExternal, PartId, PartResourceId, PartSource, Url}

final case class DatasetPart(
    resourceId:            PartResourceId,
    id:                    PartId,
    external:              PartExternal,
    entity:                Entity,
    dateCreated:           DateCreated,
    maybeUrl:              Option[Url],
    maybeSource:           Option[PartSource],
    maybeInvalidationTime: Option[InvalidationTime]
)

object DatasetPart {

  import ch.datascience.graph.model.Schemas._
  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit lazy val encoder: JsonLDEncoder[DatasetPart] = JsonLDEncoder.instance { part =>
    JsonLD.entity(
      part.resourceId.asEntityId,
      EntityTypes of (prov / "Entity", schema / "DigitalDocument"),
      renku / "external"         -> part.external.asJsonLD,
      prov / "entity"            -> part.entity.asJsonLD,
      schema / "dateCreated"     -> part.dateCreated.asJsonLD,
      schema / "url"             -> part.maybeUrl.asJsonLD,
      renku / "source"           -> part.maybeSource.asJsonLD,
      prov / "invalidatedAtTime" -> part.maybeInvalidationTime.asJsonLD
    )
  }
}
