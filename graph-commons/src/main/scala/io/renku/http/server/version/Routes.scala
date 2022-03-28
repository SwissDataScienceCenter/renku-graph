/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.http.server.version

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

trait Routes[F[_]] {
  def apply(): HttpRoutes[F]
}

class RoutesImpl[F[_]: MonadThrow](endpoint: Endpoint[F]) extends Http4sDsl[F] with Routes[F] {
  import endpoint._
  override def apply(): HttpRoutes[F] = HttpRoutes.of[F] { case GET -> Root / "version" => `GET /version` }
}

object Routes {
  def apply[F[_]: Async]: F[Routes[F]] = Endpoint[F].map(new RoutesImpl(_))
}
