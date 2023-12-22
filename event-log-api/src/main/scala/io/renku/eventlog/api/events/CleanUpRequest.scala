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

package io.renku.eventlog.api.events

import cats.Show
import cats.syntax.all._
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.literal._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.renku.events.CategoryName
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._

trait CleanUpRequest extends Product

object CleanUpRequest {

  val categoryName: CategoryName = CategoryName("CLEAN_UP_REQUEST")

  def apply(project: Project): CleanUpRequest =
    CleanUpRequest.Full(project.id, project.slug)
  def apply(maybeId: Option[projects.GitLabId], slug: projects.Slug): CleanUpRequest =
    maybeId.fold(CleanUpRequest(slug))(CleanUpRequest(_, slug))
  def apply(projectId: projects.GitLabId, projectSlug: projects.Slug): CleanUpRequest =
    CleanUpRequest.Full(projectId, projectSlug)
  def apply(projectSlug: projects.Slug): CleanUpRequest =
    CleanUpRequest.Partial(projectSlug)

  final case class Full(projectId: projects.GitLabId, projectSlug: projects.Slug) extends CleanUpRequest
  final case class Partial(projectSlug: projects.Slug)                            extends CleanUpRequest

  implicit def dispatcher[F[_]]: Dispatcher[F, CleanUpRequest] = Dispatcher.instance(categoryName)

  implicit val encoder: Encoder[CleanUpRequest] = Encoder.instance {
    case CleanUpRequest.Full(id, slug) =>
      json"""{
        "categoryName": $categoryName,
        "project": {
          "id":   $id,
          "slug": $slug
        }
      }"""
    case CleanUpRequest.Partial(slug) =>
      json"""{
        "categoryName": $categoryName,
        "project": {
          "slug": $slug
        }
      }"""
  }

  implicit val decoder: Decoder[CleanUpRequest] = Decoder.instance { cursor =>
    lazy val validateCategory = cursor.downField("categoryName").as[CategoryName] >>= {
      case `categoryName` => ().asRight
      case other          => DecodingFailure(CustomReason(s"Expected $categoryName but got $other"), cursor).asLeft
    }

    validateCategory >>
      (cursor.downField("project").downField("id").as[Option[projects.GitLabId]],
       cursor.downField("project").downField("slug").as[projects.Slug]
      ).mapN(CleanUpRequest(_, _))
  }

  implicit val show: Show[CleanUpRequest] = Show.show {
    case e: Full    => e.show
    case e: Partial => e.show
  }

  implicit val showFull: Show[Full] = Show.show { case Full(id, slug) =>
    show"projectId = $id, projectSlug = $slug"
  }
  implicit val showPartial: Show[Partial] = Show.show { case Partial(slug) => show"projectSlug = $slug" }
}
