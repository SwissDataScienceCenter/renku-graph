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

package io.renku.knowledgegraph.datasets
package details

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.data.Message
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.{GitLabUrl, RenkuUrl}
import io.renku.http.rest.Links.Href
import io.renku.logging.{ExecutionTimeRecorder, ExecutionTimeRecorderLoader}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.DatasetViewedEvent
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Response, Uri}
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /datasets/:id`(identifier: RequestedDataset, authContext: AuthContext[RequestedDataset]): F[Response[F]]
}

class EndpointImpl[F[_]: MonadThrow: Logger](
    datasetFinder:         DatasetFinder[F],
    tgClient:              triplesgenerator.api.events.Client[F],
    executionTimeRecorder: ExecutionTimeRecorder[F],
    now:                   () => Instant = () => Instant.now()
)(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl, renkuUrl: RenkuUrl)
    extends Http4sDsl[F]
    with Endpoint[F] {

  import executionTimeRecorder._

  def `GET /datasets/:id`(identifier: RequestedDataset, authContext: AuthContext[RequestedDataset]): F[Response[F]] =
    measureAndLogTime(finishedSuccessfully(identifier)) {
      datasetFinder
        .findDataset(identifier, authContext)
        .flatTap(sendDatasetViewedEvent(authContext))
        .flatMap(toHttpResult(identifier))
        .recoverWith(httpResult(identifier))
    }

  private def sendDatasetViewedEvent(authContext: AuthContext[RequestedDataset]): Option[Dataset] => F[Unit] = {
    case None => ().pure[F]
    case Some(ds) =>
      tgClient
        .send(DatasetViewedEvent.forDataset(ds.project.datasetIdentifier, authContext.maybeAuthUser.map(_.id), now))
        .handleErrorWith(err => Logger[F].error(err)(show"sending ${DatasetViewedEvent.categoryName} event failed"))
  }

  private def toHttpResult(requestedDataset: RequestedDataset): Option[Dataset] => F[Response[F]] = {
    case None          => NotFound(Message.Info.unsafeApply(show"No '$requestedDataset' dataset found"))
    case Some(dataset) => Ok(dataset.asJson(Dataset.encoder(requestedDataset)))
  }

  private def httpResult(identifier: RequestedDataset): PartialFunction[Throwable, F[Response[F]]] = {
    case NonFatal(exception) =>
      val errorMessage = Message.Error.unsafeApply(show"Finding dataset '$identifier' failed")
      Logger[F].error(exception)(errorMessage.show) >>
        InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(identifier: RequestedDataset): PartialFunction[Response[F], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      show"Finding '$identifier' dataset finished"
  }
}

object Endpoint {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Endpoint[F]] = for {
    datasetFinder                        <- DatasetFinder[F]
    tgClient                             <- triplesgenerator.api.events.Client[F]
    executionTimeRecorder                <- ExecutionTimeRecorderLoader[F]()
    implicit0(renkuApiUrl: renku.ApiUrl) <- renku.ApiUrl[F]()
    implicit0(renkuUrl: RenkuUrl)        <- RenkuUrlLoader[F]()
    implicit0(gitLabUrl: GitLabUrl)      <- GitLabUrlLoader[F]()
  } yield new EndpointImpl[F](datasetFinder, tgClient, executionTimeRecorder)

  def href(renkuApiUrl: renku.ApiUrl, identifier: RequestedDataset): Href =
    Href((Uri.unsafeFromString(renkuApiUrl.value) / "datasets" / identifier).toString)
}
