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

import ch.datascience.graph.model.RenkuBaseUrl
import ch.datascience.graph.model.publicationEvents._

final case class PublicationEvent(about:            AboutEvent,
                                  maybeDescription: Option[Description],
                                  location:         Location,
                                  name:             Name,
                                  startDate:        StartDate
)

object PublicationEvent {

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[PublicationEvent] =
    JsonLDEncoder.instance { case event @ PublicationEvent(about, maybeDescription, location, name, startDate) =>
      JsonLD.entity(
        event.asEntityId,
        EntityTypes of schema / "PublicationEvent",
        schema / "about"       -> about.asJsonLD,
        schema / "description" -> maybeDescription.asJsonLD,
        schema / "location"    -> location.asJsonLD,
        schema / "name"        -> name.asJsonLD,
        schema / "startDate"   -> startDate.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[PublicationEvent] =
    EntityIdEncoder.instance(event => renkuBaseUrl / "datasettags" / s"${event.name}@${event.location}")
}
