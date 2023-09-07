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

package io.renku.knowledgegraph.projects.images

import cats.Applicative
import cats.effect.Async
import cats.syntax.all._
import fs2.Chunk
import io.renku.knowledgegraph.multipart.PartEncoder
import org.http4s.MediaType._
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
import org.http4s.multipart.Part
import org.http4s.{DecodeFailure, DecodeResult, EntityDecoder, EntityEncoder, HttpVersion, Media, MediaType, MediaTypeMismatch, MediaTypeMissing, Response, Status}
import org.typelevel.ci._
import scodec.bits.ByteVector

object MultipartImageCodecs {

  val allowedMediaTypes: List[MediaType] =
    List(image.png, image.jpeg, image.gif, image.bmp, image.tiff, image.`vnd.microsoft.icon`)

  implicit def imagePartEncoder[F[_]]: PartEncoder[F, Image] = PartEncoder.instance {
    case (partName, Image(name, mediaType, data)) =>
      Part.fileData[F](partName, name, fs2.Stream.chunk(Chunk.byteVector(data)), `Content-Type`(mediaType))
  }

  implicit def imagePartDecoder[F[_]: Async]: EntityDecoder[F, Option[Image]] =
    EntityDecoder.decodeBy(allowedMediaTypes.head, allowedMediaTypes.tail: _*) {

      case part: Part[F] =>
        lazy val findImageData: DecodeResult[F, Option[ByteVector]] =
          DecodeResult.success {
            part.as[ByteVector].map {
              case bv if bv.isEmpty => Option.empty[ByteVector]
              case bv               => Some(bv)
            }
          }

        val filenameParameter = ci"filename"

        lazy val findImageFilename: DecodeResult[F, String] =
          part.headers
            .get[`Content-Disposition`]
            .flatMap(_.parameters.get(ci"filename"))
            .map(DecodeResult.successT(_))
            .getOrElse(DecodeResult.failureT[F, String](missingContentDisposition(filenameParameter)))

        lazy val findImageMediaType: DecodeResult[F, MediaType] =
          part.headers
            .get[`Content-Type`]
            .map(ct => DecodeResult.successT(ct.mediaType))
            .getOrElse(mediaTypeFailure[F, MediaType](part))

        findImageData.flatMap {
          case None => DecodeResult.successT(Option.empty[Image])
          case Some(data) =>
            (findImageFilename, findImageMediaType)
              .mapN(Image.apply(_, _, data).some)
        }
      case m =>
        mediaTypeFailure[F, Option[Image]](m)
    }

  private def mediaTypeFailure[F[_]: Applicative, A](media: Media[F]): DecodeResult[F, A] =
    DecodeResult.failureT(
      media.contentType
        .map(ct => MediaTypeMismatch(ct.mediaType, allowedMediaTypes.toSet))
        .getOrElse(MediaTypeMissing(allowedMediaTypes.toSet))
    )

  private def missingContentDisposition(paramName: CIString): DecodeFailure = new DecodeFailure {
    override val message: String =
      s"No '$paramName' parameter in the 'Content-Disposition' header"

    override val cause: Option[Throwable] = None

    override def toHttpResponse[G[_]](httpVersion: HttpVersion): Response[G] =
      Response(Status.BadRequest, httpVersion)
        .withEntity(message)(EntityEncoder.stringEncoder[G])
  }
}
