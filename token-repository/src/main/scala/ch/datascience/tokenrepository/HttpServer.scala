/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository

import cats.effect._
import ch.datascience.http.server.PingEndpoint
import ch.datascience.logging.ApplicationLogger
import ch.datascience.tokenrepository.repository.association.AssociateTokenEndpoint
import ch.datascience.tokenrepository.repository.deletion.DeleteTokenEndpoint
import ch.datascience.tokenrepository.repository.fetching.FetchTokenEndpoint
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds

private class HttpServer[F[_]: ConcurrentEffect](
    pingEndpoint:           PingEndpoint[F],
    fetchTokenEndpoint:     FetchTokenEndpoint[F],
    associateTokenEndpoint: AssociateTokenEndpoint[F],
    deleteTokenEndpoint:    DeleteTokenEndpoint[F]
) {
  import cats.implicits._
  import org.http4s.server.blaze._

  def run: F[ExitCode] =
    BlazeBuilder[F]
      .bindHttp(9003, "0.0.0.0")
      .mountService(new TestEndpoint[F].test, "/test")
      .mountService(pingEndpoint.ping, "/")
      .mountService(fetchTokenEndpoint.fetchToken, "/")
      .mountService(associateTokenEndpoint.associateToken, "/")
      .mountService(deleteTokenEndpoint.deleteToken, "/")
      .serve
      .compile
      .drain
      .as(ExitCode.Success)

  class TestEndpoint[F[_]: Effect] extends Http4sDsl[F] {

    val test: HttpRoutes[F] = HttpRoutes.of[F] {
      case GET -> Root / "test" => {
        ApplicationLogger.info("in test")
        Ok("abcdd")
      }
    }
  }

}
