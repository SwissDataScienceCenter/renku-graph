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

package io.renku.http.server

import cats.MonadThrow
import cats.data.NonEmptyList
import eu.timepit.refined.auto._
import io.renku.data.Message
import org.http4s.{ParseFailure, Response, Status}
import org.http4s.circe.CirceEntityCodec._

object QueryParameterTools {

  def toBadRequest[F[_]: MonadThrow]: NonEmptyList[ParseFailure] => F[Response[F]] = errors =>
    MonadThrow[F].catchNonFatal {
      Response[F](Status.BadRequest)
        .withEntity(Message.Error.unsafeApply(errors.toList.map(_.message).mkString("; ")))
    }

  def resourceNotFound[F[_]: MonadThrow]: F[Response[F]] = MonadThrow[F].catchNonFatal {
    Response(Status.NotFound)
      .withEntity(Message.Info("Resource not found"))
  }
}
