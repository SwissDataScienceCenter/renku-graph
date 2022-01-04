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

package io.renku.webhookservice

import cats.Eval
import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.http.client.RestClient
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.webhookservice.model.CommitSyncRequest
import org.http4s.Status.{Accepted, BadGateway, GatewayTimeout, ServiceUnavailable}
import org.http4s.{Status, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps

private trait CommitSyncRequestSender[F[_]] {
  def sendCommitSyncRequest(commitSyncRequest: CommitSyncRequest): F[Unit]
}

private class CommitSyncRequestSenderImpl[F[_]: Async: Temporal: Logger](
    eventLogUrl:            EventLogUrl,
    retryDelay:             FiniteDuration,
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient[F, CommitSyncRequestSender[F]](
      Throttler.noThrottling,
      retryInterval = retryInterval,
      maxRetries = maxRetries,
      requestTimeoutOverride = requestTimeoutOverride
    )
    with CommitSyncRequestSender[F] {

  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Method.POST
  import org.http4s.{Request, Response}

  def sendCommitSyncRequest(syncRequest: CommitSyncRequest): F[Unit] = for {
    uri           <- validateUri(s"$eventLogUrl/events")
    sendingResult <- send(prepareRequest(uri, syncRequest))(mapResponse) recoverWith retryDelivery(syncRequest)
    _             <- logInfo(syncRequest)
  } yield sendingResult

  private def prepareRequest(uri: Uri, commitSyncRequest: CommitSyncRequest) =
    request(POST, uri).withMultipartBuilder
      .addPart("event", commitSyncRequest.asJson)
      .build()

  private def retryDelivery(commitSyncRequest: CommitSyncRequest): PartialFunction[Throwable, F[Unit]] = {
    case UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, message) =>
      waitAndRetry(Eval.always(sendCommitSyncRequest(commitSyncRequest)), message)
    case exception @ (_: ConnectivityException | _: ClientException) =>
      waitAndRetry(Eval.always(sendCommitSyncRequest(commitSyncRequest)), exception.getMessage)
  }

  private def waitAndRetry(retry: Eval[F[Unit]], errorMessage: String) = for {
    _      <- Logger[F].error(s"Sending commit sync request event failed - retrying in $retryDelay - $errorMessage")
    _      <- Temporal[F] sleep retryDelay
    result <- retry.value
  } yield result

  private implicit lazy val entityEncoder: Encoder[CommitSyncRequest] = Encoder.instance[CommitSyncRequest] { event =>
    json"""{
      "categoryName": "COMMIT_SYNC_REQUEST",
      "project": {
        "id":   ${event.project.id.value},
        "path": ${event.project.path.value}
      }
    }"""
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Unit]] = { case (Accepted, _, _) =>
    ().pure[F]
  }

  private lazy val logInfo: CommitSyncRequest => F[Unit] = { case CommitSyncRequest(project) =>
    Logger[F].info(show"CommitSyncRequest sent for projectId = ${project.id}, projectPath = ${project.path}")
  }
}

private object CommitSyncRequestSender {

  def apply[F[_]: Async: Logger]: F[CommitSyncRequestSender[F]] = for {
    eventLogUrl <- EventLogUrl[F]()
  } yield new CommitSyncRequestSenderImpl(eventLogUrl, retryDelay = 30 seconds)
}
