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

package ch.datascience.commiteventservice.events

import cats.MonadThrow
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.RestClient
import io.circe.literal.JsonStringContext
import org.http4s.Method.PATCH
import org.http4s.Status._
import org.http4s.{Request, Response, Status}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait EventStatusPatcher[Interpretation[_]] {
  def sendDeletionStatus(projectId: projects.Id, eventId: CommitId): Interpretation[Unit]
}

private class EventStatusPatcherImpl[Interpretation[_]: MonadThrow: ConcurrentEffect: Timer](
    logger:                  Logger[Interpretation],
    eventLogUrl:             EventLogUrl
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, EventStatusPatcher[Interpretation]](Throttler.noThrottling, logger)
    with EventStatusPatcher[Interpretation] {
  override def sendDeletionStatus(projectId: projects.Id, eventId: CommitId): Interpretation[Unit] =
    for {
      uri <- validateUri(s"$eventLogUrl/events/$eventId/$projectId")
      sendingResult <-
        send(
          request(PATCH, uri).withMultipartBuilder.addPart("event", json"""{"status": "AWAITING_DELETION"}""").build()
        )(mapResponse)
    } yield sendingResult

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Ok, _, _) => ().pure[Interpretation]
  }

}

private object EventStatusPatcher {
  def apply(logger:     Logger[IO])(implicit
      executionContext: ExecutionContext,
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO]
  ): IO[EventStatusPatcherImpl[IO]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventStatusPatcherImpl[IO](logger, eventLogUrl)
}
