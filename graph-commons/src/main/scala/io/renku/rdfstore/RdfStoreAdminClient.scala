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

package io.renku.rdfstore

import RdfStoreAdminClient._
import cats.Show
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.control.Throttler
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.http.client.{HttpRequest, RestClient}
import org.http4s.Method.POST
import org.http4s.Status.{Conflict, Ok}
import org.http4s.Uri
import org.http4s.headers.`Content-Type`
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

trait RdfStoreAdminClient[F[_]] {
  def createDataset(datasetConfigFile: DatasetConfigFile): F[CreationResult]
}

object RdfStoreAdminClient {

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

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[RdfStoreAdminClient[F]] =
    AdminConnectionConfig[F]().map(new RdfStoreAdminClientImpl(_))

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      adminConfig: AdminConnectionConfig
  ): RdfStoreAdminClient[F] = new RdfStoreAdminClientImpl(adminConfig)
}

private class RdfStoreAdminClientImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    adminConnectionConfig:  AdminConnectionConfig,
    retryInterval:          FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeoutOverride:    Option[Duration] = None,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(Throttler.noThrottling,
                     Some(implicitly[SparqlQueryTimeRecorder[F]].instance),
                     retryInterval,
                     maxRetries,
                     idleTimeoutOverride,
                     requestTimeoutOverride
    )
    with RdfStoreAdminClient[F]
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
}
