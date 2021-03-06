/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.http.server

import cats.MonadError
import cats.data.NonEmptyList
import ch.datascience.http.ErrorMessage
import ch.datascience.http.ErrorMessage._
import io.circe.syntax._
import org.http4s.circe._
import org.http4s.{ParseFailure, Response, Status}

object QueryParameterTools {

  def toBadRequest[F[_]]()(
      errors:    NonEmptyList[ParseFailure]
  )(implicit ME: MonadError[F, Throwable]): F[Response[F]] = ME.pure {
    Response[F](Status.BadRequest)
      .withEntity(ErrorMessage(errors.toList.map(_.message).mkString("; ")).asJson)
  }
}
