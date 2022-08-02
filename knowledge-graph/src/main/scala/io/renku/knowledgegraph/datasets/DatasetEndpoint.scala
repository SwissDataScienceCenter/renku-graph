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

package io.renku.knowledgegraph.datasets

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.datasets.Identifier
import io.renku.http.InfoMessage._
import io.renku.http.rest.Links.Href
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait DatasetEndpoint[F[_]] {
  def getDataset(identifier: Identifier, authContext: AuthContext[Identifier]): F[Response[F]]
}

class DatasetEndpointImpl[F[_]: MonadThrow: Logger](
    datasetFinder:         DatasetFinder[F],
    renkuApiUrl:           renku.ApiUrl,
    gitLabUrl:             GitLabUrl,
    executionTimeRecorder: ExecutionTimeRecorder[F]
) extends Http4sDsl[F]
    with DatasetEndpoint[F] {

  import executionTimeRecorder._
  import org.http4s.circe._

  private implicit lazy val apiUrl: renku.ApiUrl = renkuApiUrl
  private implicit lazy val glUrl:  GitLabUrl    = gitLabUrl

  def getDataset(identifier: Identifier, authContext: AuthContext[Identifier]): F[Response[F]] = measureExecutionTime {
    datasetFinder
      .findDataset(identifier, authContext)
      .flatMap(toHttpResult(identifier))
      .recoverWith(httpResult(identifier))
  } map logExecutionTimeWhen(finishedSuccessfully(identifier))

  private def toHttpResult(
      identifier: Identifier
  ): Option[Dataset] => F[Response[F]] = {
    case None          => NotFound(InfoMessage(s"No dataset with '$identifier' id found"))
    case Some(dataset) => Ok(dataset.asJson)
  }

  private def httpResult(
      identifier: Identifier
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding dataset with '$identifier' id failed")
    Logger[F].error(exception)(errorMessage.value) >>
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(identifier: Identifier): PartialFunction[Response[F], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding '$identifier' dataset finished"
  }
}

object DatasetEndpoint {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetEndpoint[F]] = for {
    datasetFinder         <- DatasetFinder[F]
    renkuApiUrl           <- renku.ApiUrl[F]()
    gitLabUrl             <- GitLabUrlLoader[F]()
    executionTimeRecorder <- ExecutionTimeRecorder[F]()
  } yield new DatasetEndpointImpl[F](datasetFinder, renkuApiUrl, gitLabUrl, executionTimeRecorder)

  def href(renkuApiUrl: renku.ApiUrl, identifier: Identifier): Href =
    Href(renkuApiUrl / "datasets" / identifier)
}
