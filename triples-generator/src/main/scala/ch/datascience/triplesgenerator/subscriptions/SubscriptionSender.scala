/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.subscriptions

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.http.client.IORestClient
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait SubscriptionSender[Interpretation[_]] {
  def send(subscriptionUrl: SubscriptionUrl): Interpretation[Unit]
}

private class IOSubscriptionSender(
    eventLogUrl:             EventLogUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(Throttler.noThrottling, logger)
    with SubscriptionSender[IO] {

  import cats.effect._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Method.POST
  import org.http4s.Status.Accepted
  import org.http4s.circe._
  import org.http4s.{Request, Response, Status}

  override def send(subscriptionUrl: SubscriptionUrl): IO[Unit] =
    for {
      uri           <- validateUri(s"$eventLogUrl/events/subscriptions?status=READY")
      sendingResult <- send(request(POST, uri).withEntity(subscriptionUrl.asJson))(mapResponse)
    } yield sendingResult

  private implicit lazy val entityEncoder: Encoder[SubscriptionUrl] = Encoder.instance[SubscriptionUrl] { url =>
    json"""{
      "url": ${url.value}
    }"""
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Accepted, _, _) => IO.unit
  }
}

private object IOSubscriptionSender {
  def apply(
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[SubscriptionSender[IO]] =
    for {
      eventLogUrl <- EventLogUrl[IO]()
    } yield new IOSubscriptionSender(eventLogUrl, logger)

}
