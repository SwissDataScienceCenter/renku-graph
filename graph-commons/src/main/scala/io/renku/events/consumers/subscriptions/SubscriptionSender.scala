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

package io.renku.events.consumers.subscriptions

import cats.MonadThrow
import cats.effect.kernel.{Async, Temporal}
import cats.syntax.all._
import io.circe.Json
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.http.client.RestClient
import org.typelevel.log4cats.Logger

private trait SubscriptionSender[Interpretation[_]] {
  def postToEventLog(subscriptionPayload: Json): Interpretation[Unit]
}

private class SubscriptionSenderImpl[Interpretation[_]: Async: Temporal: Logger](
    eventLogUrl: EventLogUrl
) extends RestClient[Interpretation, SubscriptionSender[Interpretation]](Throttler.noThrottling)
    with SubscriptionSender[Interpretation] {

  import org.http4s.Method.POST
  import org.http4s.Status.Accepted
  import org.http4s.circe._
  import org.http4s.{Request, Response, Status}

  override def postToEventLog(subscriptionPayload: Json): Interpretation[Unit] = for {
    uri           <- validateUri(s"$eventLogUrl/subscriptions")
    sendingResult <- send(request(POST, uri).withEntity(subscriptionPayload))(mapResponse)
  } yield sendingResult

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Accepted, _, _) => MonadThrow[Interpretation].unit
  }
}

private object SubscriptionSender {
  def apply[Interpretation[_]: Async: Temporal: Logger]: Interpretation[SubscriptionSender[Interpretation]] = for {
    eventLogUrl <- EventLogUrl[Interpretation]()
  } yield new SubscriptionSenderImpl(eventLogUrl)
}
