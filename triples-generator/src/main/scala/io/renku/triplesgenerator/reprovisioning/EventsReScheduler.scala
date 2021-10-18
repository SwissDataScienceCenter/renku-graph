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

package io.renku.triplesgenerator.reprovisioning

import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.http.client.RestClient
import org.http4s.Method.POST
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait EventsReScheduler[Interpretation[_]] {
  def triggerEventsReScheduling(): Interpretation[Unit]
}

private class EventsReSchedulerImpl[Interpretation[_]: ConcurrentEffect: Timer](
    eventLogUrl:             EventLogUrl,
    logger:                  Logger[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, EventsReScheduler[Interpretation]](Throttler.noThrottling, logger)
    with EventsReScheduler[Interpretation] {

  import io.circe.literal._
  import org.http4s.Status.Accepted
  import org.http4s.{Request, Response, Status}

  override def triggerEventsReScheduling(): Interpretation[Unit] =
    for {
      uri <- validateUri(s"$eventLogUrl/events")
      sendingResult <-
        send(
          request(POST, uri).withMultipartBuilder
            .addPart("event", json"""{"categoryName": "EVENTS_STATUS_CHANGE", "newStatus": "NEW"}""")
            .build()
        )(mapResponse)
    } yield sendingResult

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Accepted, _, _) => ().pure[Interpretation]
  }
}

private object IOEventsReScheduler {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO]
  ): IO[EventsReScheduler[IO]] =
    for {
      eventLogUrl <- EventLogUrl[IO]()
    } yield new EventsReSchedulerImpl(eventLogUrl, logger)
}
