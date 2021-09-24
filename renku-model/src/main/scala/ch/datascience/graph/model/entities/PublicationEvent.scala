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

import ch.datascience.graph.model.datasets
import ch.datascience.graph.model.publicationEvents._

final case class PublicationEvent(resourceId:       ResourceId,
                                  about:            datasets.ResourceId,
                                  maybeDescription: Option[Description],
                                  name:             Name,
                                  startDate:        StartDate
)

object PublicationEvent {
  import ch.datascience.graph.model.Schemas._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  private val entityTypes = EntityTypes of schema / "PublicationEvent"

  implicit val encoder: JsonLDEncoder[PublicationEvent] = JsonLDEncoder.instance {
    case PublicationEvent(resourceId, about, maybeDescription, name, startDate) =>
      JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        (schema / "about")       -> about.asEntityId.asJsonLD,
        (schema / "description") -> maybeDescription.asJsonLD,
        (schema / "name")        -> name.asJsonLD,
        (schema / "startDate")   -> startDate.asJsonLD
      )
  }

  implicit lazy val decoder: JsonLDDecoder[PublicationEvent] = JsonLDDecoder.entity(entityTypes) { cursor =>
    import ch.datascience.graph.model.views.StringTinyTypeJsonLDDecoders._
    for {
      resourceId       <- cursor.downEntityId.as[ResourceId]
      about            <- cursor.downField(schema / "about").downEntityId.as[datasets.ResourceId]
      maybeDescription <- cursor.downField(schema / "description").as[Option[Description]]
      name             <- cursor.downField(schema / "name").as[Name]
      startDate        <- cursor.downField(schema / "startDate").as[StartDate]
    } yield PublicationEvent(resourceId, about, maybeDescription, name, startDate)
  }
}
