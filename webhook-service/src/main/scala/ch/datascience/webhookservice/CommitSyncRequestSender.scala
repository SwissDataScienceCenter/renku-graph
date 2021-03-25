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

package ch.datascience.webhookservice

import cats.Eval
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.http.client.IORestClient
import ch.datascience.http.client.RestClientError.{ConnectivityException, UnexpectedResponseException}
import ch.datascience.webhookservice.model.CommitSyncRequest
import io.chrisdavenport.log4cats.Logger
import org.http4s.Status.{Accepted, BadGateway, GatewayTimeout, ServiceUnavailable}
import org.http4s.{Status, Uri}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

private trait CommitSyncRequestSender[Interpretation[_]] {
  def sendCommitSyncRequest(commitSyncRequest: CommitSyncRequest): Interpretation[Unit]
}

private class CommitSyncRequestSenderImpl(
    eventLogUrl: EventLogUrl,
    logger:      Logger[IO],
    retryDelay:  FiniteDuration
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger)
    with CommitSyncRequestSender[IO] {

  import cats.effect._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Method.POST
  import org.http4s.{Request, Response}

  def sendCommitSyncRequest(commitSyncRequest: CommitSyncRequest): IO[Unit] = for {
    uri <- validateUri(s"$eventLogUrl/events")
    sendingResult <- send(prepareRequest(uri, commitSyncRequest))(mapResponse) recoverWith retryOnServerError(
                       Eval.always(sendCommitSyncRequest(commitSyncRequest))
                     )
  } yield sendingResult

  private def prepareRequest(uri: Uri, commitSyncRequest: CommitSyncRequest) =
    request(POST, uri).withMultipartBuilder
      .addPart("event", commitSyncRequest.asJson)
      .build()

  private def retryOnServerError(retry: Eval[IO[Unit]]): PartialFunction[Throwable, IO[Unit]] = {
    case UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, message) =>
      waitAndRetry(retry, message)
    case ConnectivityException(message, _) =>
      waitAndRetry(retry, message)
  }

  private def waitAndRetry(retry: Eval[IO[Unit]], errorMessage: String) = for {
    _      <- logger.error(s"Sending commit sync request event failed - retrying in $retryDelay - $errorMessage")
    _      <- timer sleep retryDelay
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

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Accepted, _, _) => IO.unit
  }

  private def logError(syncRequest: CommitSyncRequest): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Problems with sending $syncRequest event")
      exception.raiseError[IO, Unit]
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
