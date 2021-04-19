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

package ch.datascience.triplesgenerator.init

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.http.client.IORestClient
import ch.datascience.triplesgenerator.config.FusekiAdminConfig
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait DatasetExistenceChecker[Interpretation[_]] {
  def doesDatasetExists(): Interpretation[Boolean]
}

private class IODatasetExistenceChecker(fusekiAdminConfig: FusekiAdminConfig, logger: Logger[IO])(implicit
    executionContext:                                      ExecutionContext,
    contextShift:                                          ContextShift[IO],
    timer:                                                 Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger)
    with DatasetExistenceChecker[IO] {

  import cats.effect._
  import fusekiAdminConfig._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.dsl.io._

  override def doesDatasetExists(): IO[Boolean] =
    for {
      uri    <- validateUri(s"$fusekiBaseUrl/$$/datasets/$datasetName")
      result <- send(request(GET, uri, fusekiAdminConfig.authCredentials))(mapResponse)
    } yield result

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Boolean]] = {
    case (Ok, _, _)       => IO.pure(true)
    case (NotFound, _, _) => IO.pure(false)
  }
}
