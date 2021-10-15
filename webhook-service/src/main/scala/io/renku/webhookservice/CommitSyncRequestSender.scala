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

package io.renku.webhookservice

import cats.Eval
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.http.client.RestClient
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.webhookservice.model.CommitSyncRequest
import org.http4s.Status.{Accepted, BadGateway, GatewayTimeout, ServiceUnavailable}
import org.http4s.{Status, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

private trait CommitSyncRequestSender[Interpretation[_]] {
  def sendCommitSyncRequest(commitSyncRequest: CommitSyncRequest): Interpretation[Unit]
}

private class CommitSyncRequestSenderImpl[Interpretation[_]: ConcurrentEffect: Timer](
    eventLogUrl:             EventLogUrl,
    logger:                  Logger[Interpretation],
    retryDelay:              FiniteDuration,
    retryInterval:           FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride:  Option[Duration] = None
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, CommitSyncRequestSender[Interpretation]](Throttler.noThrottling,
                                                                                logger,
                                                                                retryInterval = retryInterval,
                                                                                maxRetries = maxRetries,
                                                                                requestTimeoutOverride =
                                                                                  requestTimeoutOverride
    )
    with CommitSyncRequestSender[Interpretation] {

  import cats.effect._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Method.POST
  import org.http4s.{Request, Response}

  def sendCommitSyncRequest(syncRequest: CommitSyncRequest): Interpretation[Unit] = for {
    uri           <- validateUri(s"$eventLogUrl/events")
    sendingResult <- send(prepareRequest(uri, syncRequest))(mapResponse) recoverWith retryDelivery(syncRequest)
    _             <- logInfo(syncRequest)
  } yield sendingResult

  private def prepareRequest(uri: Uri, commitSyncRequest: CommitSyncRequest) =
    request(POST, uri).withMultipartBuilder
      .addPart("event", commitSyncRequest.asJson)
      .build()

  private def retryDelivery(commitSyncRequest: CommitSyncRequest): PartialFunction[Throwable, Interpretation[Unit]] = {
    case UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, message) =>
      waitAndRetry(Eval.always(sendCommitSyncRequest(commitSyncRequest)), message)
    case exception @ (_: ConnectivityException | _: ClientException) =>
      waitAndRetry(Eval.always(sendCommitSyncRequest(commitSyncRequest)), exception.getMessage)
  }

  private def waitAndRetry(retry: Eval[Interpretation[Unit]], errorMessage: String) = for {
    _      <- logger.error(s"Sending commit sync request event failed - retrying in $retryDelay - $errorMessage")
    _      <- Timer[Interpretation] sleep retryDelay
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

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Accepted, _, _) => ().pure[Interpretation]
  }

  private lazy val logInfo: CommitSyncRequest => Interpretation[Unit] = { case CommitSyncRequest(project) =>
    logger.info(s"CommitSyncRequest sent for projectId = ${project.id}, projectPath = ${project.path}")
  }
}

private object CommitSyncRequestSender {

  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[CommitSyncRequestSender[IO]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new CommitSyncRequestSenderImpl(eventLogUrl, logger, retryDelay = 30 seconds)
}
