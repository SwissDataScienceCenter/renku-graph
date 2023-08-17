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

package io.renku.knowledgegraph.projects.update

import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._

private final case class ProjectUpdates(newImage: Option[Option[ImageUri]], newVisibility: Option[projects.Visibility])

private object ProjectUpdates {

  lazy val empty: ProjectUpdates = ProjectUpdates(None, None)

  implicit val encoder: Encoder[ProjectUpdates] = Encoder.instance { case ProjectUpdates(newImage, newVisibility) =>
    Json.obj(
      List(
        newImage.map(v => "image" -> v.fold(Json.Null)(_.asJson)),
        newVisibility.map(v => "visibility" -> v.asJson)
      ).flatten: _*
    )
  }

  implicit val decoder: Decoder[ProjectUpdates] =
    Decoder.instance { cur =>
      for {
        newImage <- cur
                      .downField("image")
                      .success
                      .fold(Option.empty[Option[ImageUri]].asRight[DecodingFailure]) {
                        _.as[Option[ImageUri]](blankStringToNoneDecoder(ImageUri)).map(_.some)
                      }
        newVisibility <- cur.downField("visibility").as[Option[projects.Visibility]]
      } yield ProjectUpdates(newImage, newVisibility)
    }
}
