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

package io.renku.knowledgegraph.projects.update

import MultipartRequestCodec.PartName
import cats.effect.{Async, Sync}
import cats.syntax.all._
import fs2.Stream
import io.renku.graph.model.projects
import io.renku.knowledgegraph.multipart.syntax._
import io.renku.knowledgegraph.projects.images.Image
import io.renku.knowledgegraph.projects.images.MultipartImageCodecs._
import org.http4s.Headers
import org.http4s.MediaType.image
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.typelevel.ci._

private trait MultipartRequestCodec[F[_]] extends MultipartRequestEncoder[F] with MultipartRequestDecoder[F]

private object MultipartRequestCodec {

  def apply[F[_]: Async](implicit
      encoder: MultipartRequestEncoder[F],
      decoder: MultipartRequestDecoder[F]
  ): MultipartRequestCodec[F] = new MultipartRequestCodec[F] {
    override def encode(updates:   ProjectUpdates): F[Multipart[F]]   = encoder.encode(updates)
    override def decode(multipart: Multipart[F]):   F[ProjectUpdates] = decoder.decode(multipart)
  }

  implicit def forAsync[F[_]](implicit F: Async[F]): MultipartRequestCodec[F] = apply[F]

  private[update] object PartName {
    val description = "description"
    val image       = "image"
    val keywords    = "keywords"
    val visibility  = "visibility"
  }
}

private trait MultipartRequestEncoder[F[_]] {
  def encode(updates: ProjectUpdates): F[Multipart[F]]
}

private object MultipartRequestEncoder {
  def apply[F[_]: Sync]: MultipartRequestEncoder[F] = forSync(implicitly[Sync[F]])
  implicit def forSync[F[_]](implicit F: Sync[F]): MultipartRequestEncoder[F] = new MultipartRequestEncoderImpl[F]
}

private class MultipartRequestEncoderImpl[F[_]: Sync] extends MultipartRequestEncoder[F] {

  override def encode(updates: ProjectUpdates): F[Multipart[F]] = {
    val ProjectUpdates(newDesc, newImage, newKeywords, newVisibility) = updates

    val parts =
      Vector(maybeDescPart(newDesc), maybeImagePart(newImage), newVisibility.asParts[F](PartName.visibility)).flatten
        .appendedAll(maybeKeywordsParts(newKeywords))

    Multiparts.forSync[F].flatMap(_.multipart(parts))
  }

  private def maybeDescPart(newDesc: Option[Option[projects.Description]]) =
    newDesc.map(
      _.fold(Part.formData[F](PartName.description, "").copy(body = Stream.empty): Part[F])(
        _.asPart[F](PartName.description)
      )
    )

  private def maybeImagePart(newImage: Option[Option[Image]]) =
    newImage.map(
      _.fold(
        Part[F](
          Headers(`Content-Disposition`("form-data", Map(ci"name" -> PartName.image)), `Content-Type`(image.jpeg)),
          Stream.empty
        )
      )(_.asPart[F](PartName.image))
    )

  private def maybeKeywordsParts(newKeywords: Option[Set[projects.Keyword]]) =
    newKeywords
      .map {
        case set if set.isEmpty => Vector(Part.formData[F](s"${PartName.keywords}[]", ""))
        case set                => set.toVector.asParts[F](PartName.keywords)
      }
      .getOrElse(Vector.empty)
}

private trait MultipartRequestDecoder[F[_]] {
  def decode(multipart: Multipart[F]): F[ProjectUpdates]
}

private object MultipartRequestDecoder {
  def apply[F[_]: Async]: MultipartRequestDecoder[F] = forAsync[F]
  implicit def forAsync[F[_]](implicit F: Async[F]): MultipartRequestDecoder[F] = new MultipartRequestDecoderImpl[F]
}

private class MultipartRequestDecoderImpl[F[_]: Async] extends MultipartRequestDecoder[F] {

  override def decode(multipart: Multipart[F]): F[ProjectUpdates] =
    (maybeDesc(multipart), maybeImage(multipart), maybeKeywords(multipart), maybeVisibility(multipart))
      .mapN(ProjectUpdates.apply)

  private lazy val maybeDesc: Multipart[F] => F[Option[Option[projects.Description]]] =
    _.parts
      .find(_.name contains PartName.description)
      .map(_.as[Option[projects.Description]])
      .sequence

  private lazy val maybeImage: Multipart[F] => F[Option[Option[Image]]] =
    _.parts
      .find(_.name contains PartName.image)
      .map(_.as[Option[Image]])
      .sequence

  private lazy val maybeKeywords: Multipart[F] => F[Option[Set[projects.Keyword]]] =
    _.parts.filter(_.name exists (_ startsWith PartName.keywords)) match {
      case prts if prts.isEmpty => Option.empty[Set[projects.Keyword]].pure[F]
      case prts =>
        prts
          .map(_.as[Option[projects.Keyword]])
          .sequence
          .map(_.flatten.toSet.some)
    }

  private lazy val maybeVisibility: Multipart[F] => F[Option[projects.Visibility]] =
    _.parts
      .find(_.name contains PartName.visibility)
      .map(_.as[projects.Visibility])
      .sequence
}
