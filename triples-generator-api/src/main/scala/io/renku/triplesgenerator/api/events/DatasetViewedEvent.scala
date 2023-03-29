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

package io.renku.triplesgenerator.api.events

import cats.Show
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.circe.literal._
import io.circe.DecodingFailure.Reason.CustomReason
import io.renku.events.CategoryName
import io.renku.graph.model.{datasets, persons}
import io.renku.json.JsonOps._

import java.time.Instant

final case class DatasetViewedEvent(identifier:  datasets.Identifier,
                                    dateViewed:  datasets.DateViewed,
                                    maybeUserId: Option[persons.GitLabId]
)

object DatasetViewedEvent {

  def forDataset(identifier:  datasets.Identifier,
                 maybeUserId: Option[persons.GitLabId],
                 now:         () => Instant = () => Instant.now
  ): DatasetViewedEvent =
    DatasetViewedEvent(identifier, dateViewed = datasets.DateViewed(now()), maybeUserId)

  val categoryName: CategoryName = CategoryName("DATASET_VIEWED")

  implicit val encoder: Encoder[DatasetViewedEvent] = Encoder.instance {
    case DatasetViewedEvent(identifier, dateViewed, maybeUserId) =>
      json"""{
        "categoryName": $categoryName,
        "dataset": {
          "identifier": $identifier
        },
        "date": $dateViewed
      }""" addIfDefined ("user" -> maybeUserId.map(id => json"""{"id": $id}"""))
  }

  implicit val decoder: Decoder[DatasetViewedEvent] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val validateCategory = cursor.downField("categoryName").as[CategoryName] >>= {
      case `categoryName` => ().asRight
      case other          => DecodingFailure(CustomReason(s"Expected $categoryName but got $other"), cursor).asLeft
    }

    for {
      _           <- validateCategory
      identifier  <- cursor.downField("dataset").downField("identifier").as[datasets.Identifier]
      date        <- cursor.downField("date").as[datasets.DateViewed]
      maybeUserId <- cursor.downField("user").downField("id").as[Option[persons.GitLabId]]
    } yield DatasetViewedEvent(identifier, date, maybeUserId)
  }

  implicit val show: Show[DatasetViewedEvent] = Show.show {
    case DatasetViewedEvent(identifier, dateViewed, None) =>
      show"datasetIdentifier = $identifier, date = $dateViewed"
    case DatasetViewedEvent(identifier, dateViewed, Some(userId)) =>
      show"datasetIdentifier = $identifier, date = $dateViewed, user = $userId"
  }
}
