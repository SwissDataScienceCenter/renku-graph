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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.http.client.IORestClient
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.RdfStoreConfig
import io.chrisdavenport.log4cats.Logger
import org.http4s.Uri

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait TriplesUploader[Interpretation[_]] {
  def upload(rdfTriples: RDFTriples): Interpretation[Unit]
}

private class IOTriplesUploader(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient[Any](Throttler.noThrottling, logger)
    with TriplesUploader[IO] {

  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.headers._
  import org.http4s.{Request, Response, Status}

  private lazy val dataUploadUrl = rdfStoreConfig.fusekiBaseUrl / rdfStoreConfig.datasetName / "data"

  def upload(rdfTriples: RDFTriples): IO[Unit] =
    for {
      uri <- validateUri(dataUploadUrl.value)
      _   <- send(uploadRequest(uri, rdfTriples))(mapResponse)
    } yield ()

  private def uploadRequest(uploadUri: Uri, rdfTriples: RDFTriples) =
    request(POST, uploadUri, rdfStoreConfig.authCredentials)
      .withEntity(rdfTriples.value)
      .putHeaders(`Content-Type`(`rdf+xml`))

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Ok, _, _) => IO.unit
  }
}

private object IOTriplesUploader {
  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[IOTriplesUploader] =
    RdfStoreConfig[IO]() map (new IOTriplesUploader(_, ApplicationLogger))
}
