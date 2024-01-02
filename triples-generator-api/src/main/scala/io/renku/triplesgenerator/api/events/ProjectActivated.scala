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
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.circe.literal._
import io.circe.DecodingFailure.Reason.CustomReason
import io.renku.events.CategoryName
import io.renku.graph.model.projects
import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}
import io.renku.tinytypes.constraints.InstantNotInTheFuture
import ProjectActivated.DateActivated

import java.time.Instant

final case class ProjectActivated(slug: projects.Slug, dateActivated: DateActivated)

object ProjectActivated {

  def forProject(slug: projects.Slug, now: () => Instant = () => Instant.now): ProjectActivated =
    ProjectActivated(slug, dateActivated = DateActivated(now()))

  val categoryName: CategoryName = CategoryName("PROJECT_ACTIVATED")

  implicit val encoder: Encoder[ProjectActivated] = Encoder.instance { case ProjectActivated(slug, dateActivated) =>
    json"""{
      "categoryName": $categoryName,
      "project": {
        "slug": $slug
      },
      "date": $dateActivated
    }"""
  }

  implicit val decoder: Decoder[ProjectActivated] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val validateCategory = cursor.downField("categoryName").as[CategoryName] >>= {
      case `categoryName` => ().asRight
      case other          => DecodingFailure(CustomReason(s"Expected $categoryName but got $other"), cursor).asLeft
    }

    for {
      _    <- validateCategory
      slug <- cursor.downField("project").downField("slug").as[projects.Slug]
      date <- cursor
                .downField("date")
                .as[Instant]
                .map {
                  case i if (i compareTo Instant.now()) > 0 => Instant.now()
                  case i                                    => i
                }
                .map(DateActivated)
    } yield ProjectActivated(slug, date)
  }

  implicit val show: Show[ProjectActivated] = Show.show { case ProjectActivated(slug, dateActivated) =>
    show"projectSlug = $slug, date = $dateActivated"
  }

  final class DateActivated private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object DateActivated
      extends TinyTypeFactory[DateActivated](new DateActivated(_))
      with InstantNotInTheFuture[DateActivated]
}
