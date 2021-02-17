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

package ch.datascience.webhookservice.eventprocessing.startcommit

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.IORestClient
import io.chrisdavenport.log4cats.Logger
import org.http4s.Status.{NotFound, Ok}
import org.http4s.{Request, Response, Status}

import scala.concurrent.ExecutionContext

private trait EventDetailsFinder[Interpretation[_]] {
  def checkIfExists(commitId: CommitId, projectId: projects.Id): Interpretation[Boolean]
}

private class EventDetailsFinderImpl(eventLogUrl: EventLogUrl, logger: Logger[IO])(implicit
    ME:                                           MonadError[IO, Throwable],
    executionContext:                             ExecutionContext,
    contextShift:                                 ContextShift[IO],
    timer:                                        Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger)
    with EventDetailsFinder[IO] {

  import org.http4s.Method.GET

  override def checkIfExists(commitId: CommitId, projectId: projects.Id): IO[Boolean] =
    validateUri(s"$eventLogUrl/events/$commitId/$projectId") >>= (uri => send(request(GET, uri))(mapResponse))

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Boolean]] = {
    case (Ok, _, _)       => true.pure[IO]
    case (NotFound, _, _) => false.pure[IO]
  }
}

private object EventDetailsFinder {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventDetailsFinderImpl] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventDetailsFinderImpl(eventLogUrl, logger)
}
