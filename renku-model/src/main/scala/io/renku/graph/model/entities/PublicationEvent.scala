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

package io.renku.graph.model.entities

import cats.data.ValidatedNel
import cats.syntax.all._
import io.renku.cli.model.CliPublicationEvent
import io.renku.graph.model.datasets
import io.renku.graph.model.publicationEvents._

final case class PublicationEvent(resourceId:        ResourceId,
                                  about:             About,
                                  datasetResourceId: datasets.ResourceId,
                                  maybeDescription:  Option[Description],
                                  name:              Name,
                                  startDate:         StartDate
)

object PublicationEvent {
  import io.renku.graph.model.Schemas.schema
  import io.renku.jsonld._
  import io.renku.jsonld.ontology._
  import io.renku.jsonld.syntax._

  private val entityTypes = EntityTypes of schema / "PublicationEvent"

  implicit val encoder: JsonLDEncoder[PublicationEvent] = JsonLDEncoder.instance {
    case PublicationEvent(resourceId, about, datasetResourceId, maybeDescription, name, startDate) =>
      JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        (schema / "about")       -> (about -> datasetResourceId).asJsonLD,
        (schema / "description") -> maybeDescription.asJsonLD,
        (schema / "name")        -> name.asJsonLD,
        (schema / "startDate")   -> startDate.asJsonLD
      )
  }

  private val urlEntityTypes = EntityTypes of (schema / "URL")

  private implicit lazy val datasetEdgeEncoder: JsonLDEncoder[(About, datasets.ResourceId)] = JsonLDEncoder.instance {
    case (about, datasetId) =>
      JsonLD.entity(
        about.asEntityId,
        urlEntityTypes,
        schema / "url" -> datasetId.asEntityId.asJsonLD
      )
  }

  def decoder(datasetId: Dataset.Identification): JsonLDDecoder[PublicationEvent] =
    CliPublicationEvent.decoder(datasetId.identifier, datasetId.resourceId).emap { cliEvent =>
      fromCli(cliEvent).toEither.leftMap(_.intercalate("; "))
    }

  def fromCli(cliEvent: CliPublicationEvent): ValidatedNel[String, PublicationEvent] =
    PublicationEvent(
      cliEvent.resourceId,
      cliEvent.about,
      cliEvent.datasetResourceId,
      cliEvent.maybeDescription,
      cliEvent.name,
      cliEvent.startDate
    ).validNel

  val ontology: ReverseProperty = ReverseProperty(dsClass =>
    Type.Def(
      Class(schema / "PublicationEvent"),
      ObjectProperties(
        ObjectProperty(schema / "about",
                       Type.Def(
                         Class(schema / "URL"),
                         ObjectProperty(schema / "url", dsClass)
                       )
        )
      ),
      DataProperties(
        DataProperty(schema / "description", xsd / "string"),
        DataProperty(schema / "name", xsd / "string"),
        DataProperty(schema / "startDate", xsd / "dateTime")
      )
    )
  )
}
