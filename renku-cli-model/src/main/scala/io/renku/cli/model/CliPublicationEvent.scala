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

package io.renku.cli.model

import Ontologies._
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.datasets
import io.renku.graph.model.publicationEvents._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{Schema => _, _}

final case class CliPublicationEvent(resourceId:        ResourceId,
                                     about:             About,
                                     datasetResourceId: datasets.ResourceId,
                                     maybeDescription:  Option[Description],
                                     name:              Name,
                                     startDate:         StartDate
) extends CliModel

object CliPublicationEvent {

  private val entityTypes = EntityTypes of Schema.PublicationEvent

  private implicit val datasetEdgeEncoder: JsonLDEncoder[(About, datasets.ResourceId)] = JsonLDEncoder.instance {
    case (about, datasetId) =>
      JsonLD.entity(
        about.asEntityId,
        EntityTypes of Schema.URL,
        Schema.url -> datasetId.asEntityId.asJsonLD
      )
  }

  implicit val encoder: JsonLDEncoder[CliPublicationEvent] = JsonLDEncoder.instance {
    case CliPublicationEvent(resourceId, about, datasetResourceId, maybeDescription, name, startDate) =>
      JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        Schema.about       -> (about -> datasetResourceId).asJsonLD,
        Schema.description -> maybeDescription.asJsonLD,
        Schema.name        -> name.asJsonLD,
        Schema.startDate   -> startDate.asJsonLD
      )
  }

  private def forDataset(datasetId: datasets.Identifier): Cursor => JsonLDDecoder.Result[Boolean] =
    _.downField(Schema.about).as[EntityId].map(_.show endsWith datasetId.show)

  def decoder(dataset: CliDataset): JsonLDDecoder[CliPublicationEvent] =
    decoder(dataset.identifier, dataset.resourceId)

  def decoder(
      datasetIdentifier: datasets.Identifier,
      datasetId:         datasets.ResourceId
  ): JsonLDDecoder[CliPublicationEvent] =
    JsonLDDecoder.entity(entityTypes, forDataset(datasetIdentifier)) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        about            <- cursor.downField(Schema.about).as(datasetEdgeDecoder(datasetId))
        maybeDescription <- cursor.downField(Schema.description).as[Option[Description]]
        name             <- cursor.downField(Schema.name).as[Name]
        startDate        <- cursor.downField(Schema.startDate).as[StartDate]
      } yield CliPublicationEvent(resourceId, about, datasetId, maybeDescription, name, startDate)
    }

  private def datasetEdgeDecoder(datasetId: datasets.ResourceId): JsonLDDecoder[About] =
    JsonLDDecoder.entity(EntityTypes of Schema.URL) { cursor =>
      for {
        about <- cursor.downEntityId.as[About]
        url   <- cursor.downField(Schema.url).downEntityId.as[datasets.ResourceId]
        _ <- if (url == datasetId) ().asRight
             else DecodingFailure(show"Publication Event $about does not point to $url", Nil).asLeft
      } yield about
    }
}
