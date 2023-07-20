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

package io.renku.http.server.endpoint

import cats.syntax.all._
import cats.{Applicative, Eval}
import io.circe.syntax._
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.headers.Accept
import org.http4s.{MediaRange, MediaType, Request, Response, Status}

trait ResponseTools {

  import ResponseTools._

  implicit class MediaTypeOps(mediaType: MediaType) {
    def -->[F[_]: Applicative](response: => F[Response[F]]): AcceptMapping[F] =
      AcceptMapping[F](mediaType, Eval.later(response))
  }

  def whenAccept[F[_]: Applicative](
      mapping: AcceptMapping[F]*
  )(default: => F[Response[F]])(implicit request: Request[F]): F[Response[F]] = {
    def notSupported(accept: Accept) = Response[F](Status.BadRequest)
      .withEntity(
        ErrorMessage(s"Accept: ${accept.values.map(_.mediaRange.toString()).intercalate(", ")} not supported").asJson
      )
      .pure[F]

    request.headers
      .get[Accept]
      .map { accept =>
        accept.values.toList.map(_.mediaRange) match {
          case Nil                     => default
          case MediaRange.`*/*` :: Nil => default
          case acceptRanges =>
            acceptRanges
              .collect(mr => mapping.find(_.mediaType.satisfiedBy(mr)))
              .flatten
              .headOption
              .map(_.response.value)
              .getOrElse(notSupported(accept))
        }
      }
      .getOrElse(default)
  }
}

object ResponseTools extends ResponseTools {
  final case class AcceptMapping[F[_]](mediaType: MediaType, response: Eval[F[Response[F]]])
}
