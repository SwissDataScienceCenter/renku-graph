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

import MultipartRequestCodec.PartName
import cats.MonadThrow
import cats.effect.{Async, Sync}
import cats.syntax.all._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty
import fs2.{Chunk, Stream}
import io.renku.graph.model.projects
import io.renku.knowledgegraph.projects.update.ProjectUpdates.Image
import io.renku.tinytypes.{From, TinyType}
import org.http4s.MediaType.image
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{Headers, MediaType}
import org.typelevel.ci.CIStringSyntax
import scodec.bits.ByteVector

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
      Vector(maybeDescPart(newDesc), maybeImagePart(newImage), maybeVisibilityPart(newVisibility)).flatten
        .appendedAll(maybeKeywordsParts(newKeywords))

    Multiparts.forSync[F].flatMap(_.multipart(parts))
  }

  private def maybeDescPart(newDesc: Option[Option[projects.Description]]) =
    newDesc.map(
      _.fold(Part.formData[F](PartName.description, "").copy(body = Stream.empty): Part[F])(v =>
        Part.formData[F](PartName.description, v.value)
      )
    )

  private def maybeImagePart(newImage: Option[Option[Image]]) =
    newImage.map(
      _.fold(
        Part[F](
          Headers(`Content-Disposition`("form-data", Map(ci"name" -> PartName.image)), `Content-Type`(image.jpeg)),
          Stream.empty
        )
      ) { case Image(name, mediaType, data) =>
        Part.fileData[F](PartName.image, name, fs2.Stream.chunk(Chunk.byteVector(data)), `Content-Type`(mediaType))
      }
    )

  private def maybeKeywordsParts(newKeywords: Option[Set[projects.Keyword]]) =
    newKeywords
      .map {
        case set if set.isEmpty => Vector(Part.formData[F](s"${PartName.keywords}[]", ""))
        case set                => set.toVector.map(v => Part.formData[F](s"${PartName.keywords}[]", v.value))
      }
      .getOrElse(Vector.empty)

  private def maybeVisibilityPart(newVisibility: Option[projects.Visibility]) =
    newVisibility.map(v => Part.formData[F](PartName.visibility, v.value))
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
      .map(_.as[String].flatMap(blankStringToNone(projects.Description)))
      .sequence

  private lazy val maybeImage: Multipart[F] => F[Option[Option[Image]]] =
    _.parts
      .find(_.name contains PartName.image)
      .map { part =>
        findImageData(part) >>= {
          case None       => Option.empty[Image].pure[F]
          case Some(data) => (findImageFilename(part), findImageMediaType(part)).mapN(Image.apply(_, _, data).some)
        }
      }
      .sequence

  private lazy val findImageData: Part[F] => F[Option[ByteVector]] =
    _.as[ByteVector].map {
      case bv if bv.isEmpty => Option.empty[ByteVector]
      case bv               => Some(bv)
    }

  private lazy val findImageFilename: Part[F] => F[String] =
    _.headers
      .get[`Content-Disposition`]
      .flatMap(_.parameters.get(ci"filename"))
      .fold(new Exception(s"No filename on the ${PartName.image} part").raiseError[F, String])(_.pure[F])

  private lazy val findImageMediaType: Part[F] => F[MediaType] =
    _.headers
      .get[`Content-Type`]
      .fold(new Exception(s"No media type on the ${PartName.image} part").raiseError[F, MediaType])(_.mediaType.pure[F])

  private lazy val maybeKeywords: Multipart[F] => F[Option[Set[projects.Keyword]]] =
    _.parts.filter(_.name exists (_ startsWith PartName.keywords)) match {
      case prts if prts.isEmpty => Option.empty[Set[projects.Keyword]].pure[F]
      case prts =>
        prts
          .map(_.as[String].flatMap(blankStringToNone(projects.Keyword)))
          .sequence
          .map(_.flatten.toSet.some)
    }

  private lazy val maybeVisibility: Multipart[F] => F[Option[projects.Visibility]] =
    _.parts
      .find(_.name contains PartName.visibility)
      .map(_.as[String].flatMap(projects.Visibility.from(_).fold(_.raiseError[F, projects.Visibility], _.pure[F])))
      .sequence

  private type NonBlank = String Refined NonEmpty

  private def blankStringToNone[TT <: TinyType { type V = String }](implicit
      tinyTypeFactory: From[TT]
  ): String => F[Option[TT]] =
    (blankToNone andThen toOption[TT])(_)

  private lazy val blankToNone: String => Option[NonBlank] = _.trim match {
    case ""       => None
    case nonBlank => RefType.applyRef[NonBlank](nonBlank).fold(_ => None, Option.apply)
  }

  private def toOption[TT <: TinyType { type V = String }](implicit
      ttFactory: From[TT]
  ): Option[NonBlank] => F[Option[TT]] = {
    case Some(nonBlank) => MonadThrow[F].fromEither(ttFactory.from(nonBlank.value).map(Option.apply))
    case None           => Option.empty[TT].pure[F]
  }
}
