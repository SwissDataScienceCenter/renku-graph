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

package ch.datascience.rdfstore.entities

import ch.datascience.graph.model.datasets.{DateCreated, PartExternal, PartId, PartSource, Url}

case class DatasetPart(
    id:          PartId,
    external:    PartExternal,
    entity:      Entity,
    dateCreated: DateCreated,
    maybeUrl:    Option[Url],
    maybeSource: Option[PartSource]
)

object DatasetPart {

  import ch.datascience.graph.model.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[DatasetPart] =
    JsonLDEncoder.instance {
      case part: DatasetPart with HavingInvalidationTime =>
        JsonLD.entity(
          part.asEntityId,
          EntityTypes of (prov / "Entity", schema / "DigitalDocument"),
          renku / "external"         -> part.external.asJsonLD,
          prov / "entity"            -> part.entity.asJsonLD,
          schema / "dateCreated"     -> part.dateCreated.asJsonLD,
          schema / "url"             -> part.maybeUrl.asJsonLD,
          renku / "source"           -> part.maybeSource.asJsonLD,
          prov / "invalidatedAtTime" -> part.invalidationTime.asJsonLD
        )
      case part @ DatasetPart(_, external, entity, dateCreated, maybeUrl, maybeSource) =>
        JsonLD.entity(
          part.asEntityId,
          EntityTypes of (prov / "Entity", schema / "DigitalDocument"),
          renku / "external"     -> external.asJsonLD,
          prov / "entity"        -> entity.asJsonLD,
          schema / "dateCreated" -> dateCreated.asJsonLD,
          schema / "url"         -> maybeUrl.asJsonLD,
          renku / "source"       -> maybeSource.asJsonLD
        )
    }

  private implicit lazy val sourceUrlEncoder: JsonLDEncoder[PartSource] =
    JsonLDEncoder.instance(v => JsonLD.fromString(v.value))

  implicit def entityIdEncoder[D <: DatasetPart](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[D] =
    EntityIdEncoder.instance(part => renkuBaseUrl / "dataset-files" / part.id / part.entity.location)
}
