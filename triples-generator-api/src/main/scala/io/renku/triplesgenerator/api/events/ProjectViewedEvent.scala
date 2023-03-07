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
import io.renku.graph.model.projects

import java.time.Instant

final case class ProjectViewedEvent(path: projects.Path, dateViewed: projects.DateViewed)

object ProjectViewedEvent {

  def forProject(path: projects.Path, now: () => Instant = () => Instant.now): ProjectViewedEvent =
    ProjectViewedEvent(path, dateViewed = projects.DateViewed(now()))

  val categoryName = CategoryName("PROJECT_VIEWED")

  implicit val encoder: Encoder[ProjectViewedEvent] = Encoder.instance { case ProjectViewedEvent(path, dateViewed) =>
    json"""{
      "categoryName": $categoryName,
      "project": {
        "path": $path
      },
      "date": $dateViewed
    }"""
  }

  implicit val decoder: Decoder[ProjectViewedEvent] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val validateCategory = cursor.downField("categoryName").as[CategoryName] >>= {
      case `categoryName` => ().asRight
      case other          => DecodingFailure(CustomReason(s"Expected $categoryName but got $other"), cursor).asLeft
    }

    for {
      _    <- validateCategory
      path <- cursor.downField("project").downField("path").as[projects.Path]
      date <- cursor.downField("date").as[projects.DateViewed]
    } yield ProjectViewedEvent(path, date)
  }

  implicit val show: Show[ProjectViewedEvent] = Show.show { case ProjectViewedEvent(path, dateViewed) =>
    show"projectPath = $path, date = $dateViewed"
  }
}
