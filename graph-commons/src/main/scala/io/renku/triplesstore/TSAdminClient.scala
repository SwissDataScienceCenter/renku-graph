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

package io.renku.triplesstore

import TSAdminClient._
import cats.Show
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.control.Throttler
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.http.client.{HttpRequest, RestClient}
import org.http4s.Method.{GET, POST}
import org.http4s.Status.{Conflict, NotFound, Ok}
import org.http4s.Uri
import org.http4s.headers.`Content-Type`
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

trait TSAdminClient[F[_]] {
  def createDataset(datasetConfigFile: DatasetConfigFile): F[CreationResult]
  def checkDatasetExists(datasetName:  DatasetName):       F[Boolean]
}

object TSAdminClient {

  sealed trait CreationResult extends Product with Serializable
  object CreationResult {
    case object Created extends CreationResult
    type Created = Created.type
    case object Existed extends CreationResult
    type Existed = Existed.type

    implicit val show: Show[CreationResult] = Show.show {
      case Created => "created"
      case Existed => "existed"
    }
  }

  def apply[F[_]: Async: Logger]: F[TSAdminClient[F]] =
    AdminConnectionConfig[F]().map(new TSAdminClientImpl(_))

  def apply[F[_]: Async: Logger](adminConfig: AdminConnectionConfig): TSAdminClient[F] =
    new TSAdminClientImpl(adminConfig)
}

private class TSAdminClientImpl[F[_]: Async: Logger](
    adminConnectionConfig:  AdminConnectionConfig,
    retryInterval:          FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeoutOverride:    Option[Duration] = None,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(Throttler.noThrottling,
                     maybeTimeRecorder = None,
                     retryInterval,
                     maxRetries,
                     idleTimeoutOverride,
                     requestTimeoutOverride
    )
    with TSAdminClient[F]
    with RdfMediaTypes {

  import adminConnectionConfig._

  override def createDataset(datasetConfigFile: DatasetConfigFile): F[CreationResult] = for {
    uri          <- validateUri(s"$fusekiUrl/$$/datasets")
    uploadResult <- send(datasetCreationRequest(uri, datasetConfigFile))(mapCreationResponse)
  } yield uploadResult

  private def datasetCreationRequest(uri: Uri, configFile: DatasetConfigFile) = HttpRequest[F](
    request(POST, uri, authCredentials)
      .withEntity(configFile.show)
      .putHeaders(`Content-Type`(`text/turtle`)),
    name = "dataset creation"
  )

  private lazy val mapCreationResponse: ResponseMapping[CreationResult] = {
    case (Ok, _, _)       => CreationResult.Created.pure[F].widen
    case (Conflict, _, _) => CreationResult.Existed.pure[F].widen
  }

  override def checkDatasetExists(datasetName: DatasetName): F[Boolean] = for {
    uri          <- validateUri(show"$fusekiUrl/$$/datasets/$datasetName")
    uploadResult <- send(request(GET, uri, authCredentials))(mapDatasetExistenceCheckResponse)
  } yield uploadResult

  private lazy val mapDatasetExistenceCheckResponse: ResponseMapping[Boolean] = {
    case (Ok, _, _)       => true.pure[F]
    case (NotFound, _, _) => false.pure[F]
  }
}
