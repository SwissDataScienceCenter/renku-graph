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
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.literal._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.renku.events.CategoryName
import io.renku.events.consumers.Project
import io.renku.graph.model.projects

final case class CleanUpEvent(project: Project)

object CleanUpEvent {

  val categoryName: CategoryName = CategoryName("CLEAN_UP")

  implicit val encoder: Encoder[CleanUpEvent] = Encoder.instance { case CleanUpEvent(Project(id, path)) =>
    json"""{
      "categoryName": $categoryName,
      "project": {
        "id":   $id,
        "path": $path
      }
    }"""
  }

  implicit val decoder: Decoder[CleanUpEvent] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val validateCategory = cursor.downField("categoryName").as[CategoryName] >>= {
      case `categoryName` => ().asRight
      case other          => DecodingFailure(CustomReason(s"Expected $categoryName but got $other"), cursor).asLeft
    }

    for {
      _    <- validateCategory
      id   <- cursor.downField("project").downField("id").as[projects.GitLabId]
      path <- cursor.downField("project").downField("path").as[projects.Path]
    } yield CleanUpEvent(Project(id, path))
  }

  implicit val show: Show[CleanUpEvent] = Show[Project].contramap(_.project)
}
