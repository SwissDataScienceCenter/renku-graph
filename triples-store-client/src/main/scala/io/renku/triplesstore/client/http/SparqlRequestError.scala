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

package io.renku.triplesstore.client.http

import cats.effect.Concurrent
import cats.syntax.all._
import org.http4s.{EntityDecoder, Response, Status}

final case class SparqlRequestError(req: String, status: Status, body: String)
    extends RuntimeException(s"Sparql request '$req' failed with status=$status: $body") {
  override def fillInStackTrace(): Throwable = this
}

object SparqlRequestError {
  def apply[F[_]: Concurrent](req: String, resp: Response[F]): F[SparqlRequestError] =
    EntityDecoder.decodeText(resp).map(str => SparqlRequestError(req, resp.status, str))
}
