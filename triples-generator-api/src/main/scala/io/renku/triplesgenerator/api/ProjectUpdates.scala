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

package io.renku.triplesgenerator.api

import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._

final case class ProjectUpdates(newDescription: Option[Option[projects.Description]],
                                newImages:      Option[List[ImageUri]],
                                newKeywords:    Option[Set[projects.Keyword]],
                                newVisibility:  Option[projects.Visibility]
)

object ProjectUpdates {

  lazy val empty: ProjectUpdates =
    ProjectUpdates(newDescription = None, newImages = None, newKeywords = None, newVisibility = None)

  implicit val encoder: Encoder[ProjectUpdates] = Encoder.instance {
    case ProjectUpdates(newDescription, newImages, newKeywords, newVisibility) =>
      Json.obj(
        List(
          newDescription.map(v => "description" -> v.fold(Json.Null)(_.asJson)),
          newImages.map(v => "images" -> v.asJson),
          newKeywords.map(v => "keywords" -> v.asJson),
          newVisibility.map(v => "visibility" -> v.asJson)
        ).flatten: _*
      )
  }

  implicit val decoder: Decoder[ProjectUpdates] = Decoder.instance { cur =>
    for {
      newDesc <- cur
                   .downField("description")
                   .success
                   .fold(Option.empty[Option[projects.Description]].asRight[DecodingFailure]) {
                     _.as[Option[projects.Description]].map(_.some)
                   }
      newImages     <- cur.downField("images").as[Option[List[ImageUri]]]
      newKeywords   <- cur.downField("keywords").as[Option[List[projects.Keyword]]].map(_.map(_.toSet))
      newVisibility <- cur.downField("visibility").as[Option[projects.Visibility]]
    } yield ProjectUpdates(newDesc, newImages, newKeywords, newVisibility)
  }
}
