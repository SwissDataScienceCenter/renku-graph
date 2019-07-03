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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.http.client.{BasicAuthCredentials, IORestClient}
import ch.datascience.triplesgenerator.config.FusekiUserConfig
import io.chrisdavenport.log4cats.Logger
import org.http4s.Uri

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DatasetTruncator[Interpretation[_]] {
  def truncateDataset: Interpretation[Unit]
}

private class IODatasetTruncator private (
    updateUri:               Uri,
    authCredentials:         BasicAuthCredentials,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(Throttler.noThrottling, logger)
    with DatasetTruncator[IO] {

  import cats.effect._
  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s._
  import org.http4s.dsl.io._
  import org.http4s.headers.`Content-Type`

  override def truncateDataset: IO[Unit] = send(updateRequest(updateUri))(mapResponse)

  private def updateRequest(uri: Uri) =
    request(POST, uri, authCredentials)
      .withEntity("update=DELETE { ?s ?p ?o} WHERE { ?s ?p ?o}")
      .putHeaders(`Content-Type`(`x-www-form-urlencoded`): Header)

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Ok, _, _)           => IO.unit
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
    case (Forbidden, _, _)    => IO.raiseError(UnauthorizedException)
  }
}

private object IODatasetTruncator {

  import IORestClient._

  def apply(
      fusekiUserConfig:        FusekiUserConfig,
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[DatasetTruncator[IO]] =
    for {
      uri <- validateUri(s"${fusekiUserConfig.fusekiBaseUrl}/${fusekiUserConfig.datasetName}/update")
    } yield new IODatasetTruncator(uri, fusekiUserConfig.authCredentials, logger)
}
