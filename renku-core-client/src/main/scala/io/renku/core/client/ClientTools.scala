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

package io.renku.core.client

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.{Decoder, DecodingFailure}
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Request, Response, Status}

private object ClientTools {
  def apply[F[_]: Concurrent]: ClientTools[F] = new ClientTools[F]
}

private class ClientTools[F[_]: Concurrent] {

  def toResult[T](resp: Response[F])(implicit dec: Decoder[T]): F[Result[T]] = {

    val resultDecoder = Decoder.instance { cur =>
      val maybeSuccess = cur.downField("result").success.map(_.as[T].map(Result.success[T]))
      val maybeError   = cur.downField("error").success.map(_.as[Result.Failure.Detailed])
      val illegalPayload =
        DecodingFailure(CustomReason(s"Cannot decode core's response ${cur.value.spaces2}"), cur).asLeft

      maybeSuccess orElse maybeError getOrElse illegalPayload
    }

    implicit val entityDec: EntityDecoder[F, Result[T]] = jsonOf[F, Result[T]](implicitly[Concurrent[F]], resultDecoder)

    resp.as[Result[T]].handleError(ex => Result.failure[T](ex.getMessage))
  }

  def toFailure[T](message: String): ((Status, Request[F], Response[F])) => F[Result[T]] = { case (status, req, resp) =>
    Result
      .failure[T](s"$message: ${req.method} ${req.pathInfo.renderString} responded with: $status, ${resp.as[String]}")
      .pure[F]
  }
}
