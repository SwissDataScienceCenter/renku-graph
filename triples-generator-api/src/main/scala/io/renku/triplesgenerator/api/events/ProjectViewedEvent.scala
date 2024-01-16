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

package io.renku.triplesgenerator.api.events

import cats.Show
import cats.syntax.all._
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.literal._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.renku.events.CategoryName
import io.renku.graph.model.{persons, projects}
import io.renku.json.JsonOps._

import java.time.Instant

final case class ProjectViewedEvent(slug: projects.Slug, dateViewed: projects.DateViewed, maybeUserId: Option[UserId])

object ProjectViewedEvent {

  def forProject(slug: projects.Slug, now: () => Instant = () => Instant.now): ProjectViewedEvent =
    ProjectViewedEvent(slug, dateViewed = projects.DateViewed(now()), maybeUserId = None)

  def forProjectAndUserId(slug:   projects.Slug,
                          userId: Option[persons.GitLabId],
                          now:    () => Instant = () => Instant.now
  ): ProjectViewedEvent =
    ProjectViewedEvent(slug, dateViewed = projects.DateViewed(now()), userId.map(UserId(_)))

  def forProjectAndUserEmail(slug:      projects.Slug,
                             userEmail: persons.Email,
                             now:       () => Instant = () => Instant.now
  ): ProjectViewedEvent =
    ProjectViewedEvent(slug, dateViewed = projects.DateViewed(now()), Some(UserId(userEmail)))

  val categoryName: CategoryName = CategoryName("PROJECT_VIEWED")

  implicit val encoder: Encoder[ProjectViewedEvent] = Encoder.instance {
    case ProjectViewedEvent(slug, dateViewed, maybeUserId) =>
      json"""{
        "categoryName": $categoryName,
        "project": {
          "slug": $slug
        },
        "date": $dateViewed
      }""" addIfDefined "user" -> maybeUserId
  }

  implicit val decoder: Decoder[ProjectViewedEvent] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val validateCategory = cursor.downField("categoryName").as[CategoryName] >>= {
      case `categoryName` => ().asRight
      case other          => DecodingFailure(CustomReason(s"Expected $categoryName but got $other"), cursor).asLeft
    }

    for {
      _           <- validateCategory
      slug        <- cursor.downField("project").downField("slug").as[projects.Slug]
      maybeUserId <- cursor.downField("user").as[Option[UserId]]
      date <- cursor
                .downField("date")
                .as[Instant]
                .map {
                  case i if (i compareTo Instant.now()) > 0 => Instant.now()
                  case i                                    => i
                }
                .map(projects.DateViewed)
    } yield ProjectViewedEvent(slug, date, maybeUserId)
  }

  implicit val show: Show[ProjectViewedEvent] = Show.show {
    case ProjectViewedEvent(slug, dateViewed, None) =>
      show"projectSlug = $slug, date = $dateViewed"
    case ProjectViewedEvent(slug, dateViewed, Some(userId)) =>
      show"projectSlug = $slug, date = $dateViewed, user = $userId"
  }
}
