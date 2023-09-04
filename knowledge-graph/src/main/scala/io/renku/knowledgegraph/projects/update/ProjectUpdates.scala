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

import ProjectUpdates.Image
import cats.Show
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.tinytypes.{From, TinyType}
import org.http4s.MediaType
import scodec.bits.ByteVector

private final case class ProjectUpdates(newDescription: Option[Option[projects.Description]],
                                        newImage:       Option[Option[Image]],
                                        newKeywords:    Option[Set[projects.Keyword]],
                                        newVisibility:  Option[projects.Visibility]
) {

  lazy val onlyGLUpdateNeeded: Boolean =
    (newImage orElse newVisibility).isDefined &&
      (newDescription orElse newKeywords).isEmpty

  lazy val glUpdateNeeded: Boolean =
    (newImage orElse newVisibility).isDefined

  lazy val coreUpdateNeeded: Boolean =
    (newDescription orElse newKeywords).isDefined
}

private object ProjectUpdates {

  lazy val empty: ProjectUpdates = ProjectUpdates(None, None, None, None)

  final case class Image(name: String, mediaType: MediaType, data: ByteVector)
  object Image {
    implicit val show: Show[Image] = Show.show { case Image(name, mediaType, _) =>
      show"$name ($mediaType)"
    }
  }

  implicit val jsonEncoder: Encoder[ProjectUpdates] = Encoder.instance {
    case ProjectUpdates(newDescription, _, newKeywords, newVisibility) =>
      Json.obj(
        List(
          newDescription.map(v => "description" -> v.fold(Json.Null)(_.asJson)),
          newKeywords.map(v => "keywords" -> v.asJson),
          newVisibility.map(v => "visibility" -> v.asJson)
        ).flatten: _*
      )
  }

  implicit val jsonDecoder: Decoder[ProjectUpdates] = Decoder.instance { cur =>
    def toOptionOfOption[T <: TinyType { type V = String }](prop: String, ttFactory: From[T]) =
      cur
        .downField(prop)
        .success
        .fold(Option.empty[Option[T]].asRight[DecodingFailure]) {
          _.as[Option[T]](blankStringToNoneDecoder(ttFactory)).map(_.some)
        }

    for {
      newDesc       <- toOptionOfOption("description", projects.Description)
      newKeywords   <- cur.downField("keywords").as[Option[List[projects.Keyword]]].map(_.map(_.toSet))
      newVisibility <- cur.downField("visibility").as[Option[projects.Visibility]]
    } yield ProjectUpdates(newDesc, newImage = None, newKeywords, newVisibility)
  }

  implicit val show: Show[ProjectUpdates] = Show.show {
    case ProjectUpdates(newDescription, newImage, newKeywords, newVisibility) =>
      def showOption[T](opt: Option[T])(implicit show: Show[T]) =
        opt.fold(ifEmpty = "none")(_.show)

      List(
        newDescription.map(v => s"description=${showOption(v)}"),
        newImage.map(v => s"image=${showOption(v)}"),
        newKeywords.map(v => s"keywords=${v.mkString(", ")}"),
        newVisibility.map(v => s"visibility=$v")
      ).flatten.mkString(", ")
  }
}
