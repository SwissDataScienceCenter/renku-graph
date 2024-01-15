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

package io.renku.knowledgegraph.multipart

import cats.effect.Concurrent
import cats.syntax.all._
import cats.{Functor, MonadThrow}
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.renku.tinytypes.{From, TinyType, TinyTypeFactory}
import org.http4s.multipart.{Multipart, Part}
import org.http4s.{DecodeFailure, DecodeResult, EntityDecoder, MalformedMessageBodyFailure}

object syntax {

  final implicit class EncoderOps[A](private val value: A) extends AnyVal {

    def asPart[F[_]](partName: String)(implicit encoder: PartEncoder[F, A]): Part[F] =
      encoder(partName, value)
  }

  final implicit class EncoderFOps[M[_]: Functor, A](private val value: M[A]) {

    def asParts[F[_]](partName: String)(implicit encoder: PartEncoder[F, A]): M[Part[F]] =
      value.map(_.asPart[F](s"$partName$partNameSuffix"))

    private lazy val partNameSuffix: String = value match {
      case _: Iterable[_] => "[]"
      case _ => ""
    }
  }

  final implicit class MultipartOps[F[_]: MonadThrow](private val multipart: Multipart[F]) {

    def part(partName: String): F[Part[F]] =
      findPart(partName).fold(new Exception(s"No '$partName' in the request").raiseError[F, Part[F]])(_.pure[F])

    def findPart(partName: String): Option[Part[F]] =
      multipart.parts.find(_.name contains partName)

    def filterParts(partName: String): List[Part[F]] =
      multipart.parts.filter(_.name exists (_ startsWith partName)).toList
  }

  implicit def optionalStringTinyTypeEntityDecoder[F[_]: Concurrent, TT <: TinyType { type V = String }](implicit
      ttFactory: From[TT]
  ): EntityDecoder[F, Option[TT]] =
    EntityDecoder.text[F].flatMapR { v =>
      type NonBlank = String Refined NonEmpty

      lazy val blankToNone = DecodeResult.successT {
        v.trim match {
          case ""       => None
          case nonBlank => RefType.applyRef[NonBlank](nonBlank).fold(_ => None, Option.apply)
        }
      }

      lazy val toOption: Option[NonBlank] => DecodeResult[F, Option[TT]] = {
        case None => DecodeResult.successT(Option.empty[TT])
        case Some(nonBlank) =>
          ttFactory
            .from(nonBlank.value)
            .fold(
              err => DecodeResult.failureT(MalformedMessageBodyFailure(err.getMessage)),
              suc => DecodeResult.successT(Option(suc))
            )
      }

      blankToNone >>= toOption
    }

  implicit def listTTEntityDecoder[F[_]: Concurrent, TT <: TinyType { type V = String }](implicit
      ttFactory: From[TT]
  ): EntityDecoder[F, List[TT]] =
    EntityDecoder.text[F].flatMapR {
      case t if t.isEmpty => DecodeResult.successT(List.empty[TT])
      case t =>
        t.split(",")
          .toList
          .map(_.trim)
          .map(ttFactory.from)
          .sequence
          .fold(
            err => DecodeResult.failureT(MalformedMessageBodyFailure(err.getMessage)),
            DecodeResult.successT(_)
          )
    }

  implicit def stringTinyTypeEntityDecoder[F[_]: Concurrent, TT <: TinyType { type V = String }](implicit
      ttFactory: From[TT]
  ): EntityDecoder[F, TT] =
    EntityDecoder.text[F].flatMapR {
      ttFactory
        .from(_)
        .fold(
          err => DecodeResult.failureT(MalformedMessageBodyFailure(err.getMessage)),
          DecodeResult.successT(_)
        )
    }

  implicit def intTinyTypeEntityDecoder[F[_]: Concurrent, TT <: TinyType { type V = Int }](implicit
      ttFactory: TinyTypeFactory[TT]
  ): EntityDecoder[F, TT] =
    EntityDecoder.text[F].flatMapR { v =>
      DecodeResult.apply {
        v.toIntOption
          .toRight(MalformedMessageBodyFailure(s"'$v' is not valid ${ttFactory.typeName}"))
          .flatMap(ttFactory.from(_).leftMap(err => MalformedMessageBodyFailure(err.getMessage)))
          .leftWiden[DecodeFailure]
          .pure[F]
      }
    }

  implicit def jsonEntityDecoder[F[_]: Concurrent]: EntityDecoder[F, Json] =
    org.http4s.circe.jsonOf[F, Json]
}
