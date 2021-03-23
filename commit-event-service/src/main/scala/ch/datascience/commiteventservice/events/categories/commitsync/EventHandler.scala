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

package ch.datascience.commiteventservice.events.categories.commitsync

import cats.MonadError
import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.MissedEventsGenerator
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.graph.model.events.{CategoryName, CommitId, LastSyncedDate}
import ch.datascience.logging.ExecutionTimeRecorder
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: MonadError[*[_], Throwable]](
    override val categoryName: CategoryName,
    missedEventGenerator:      MissedEventsGenerator[Interpretation],
    logger:                    Logger[Interpretation]
)(implicit
    contextShift: ContextShift[Interpretation],
    concurrent:   Concurrent[Interpretation]
) extends consumers.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import missedEventGenerator._

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = {
    for {
      _ <- fromEither[Interpretation](request.event.validateCategoryName)
      event <-
        fromEither[Interpretation](
          request.event.as[CommitSyncEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
        )
      result <- (contextShift.shift *> concurrent
                  .start(generateMissedEvents(event) recoverWith logError(event))).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger log event)
                  .leftSemiflatTap(logger log event)
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: CommitSyncEvent => String = { event =>
    s"id = ${event.id}, projectId = ${event.project.id}, projectPath = ${event.project.path}, lastSynced = ${event.lastSynced}"
  }

  private def logError(event: CommitSyncEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.logError(event, exception)
      exception.raiseError[Interpretation, Unit]
  }

  private implicit val eventDecoder: Decoder[CommitSyncEvent] = cursor =>
    for {
      id         <- cursor.downField("id").as[CommitId]
      project    <- cursor.downField("project").as[CommitProject]
      lastSynced <- cursor.downField("lastSynced").as[LastSyncedDate]
    } yield CommitSyncEvent(categoryName, id, project, lastSynced)

  private implicit lazy val projectDecoder: Decoder[CommitProject] = cursor =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield CommitProject(id, path)
}

private[events] object EventHandler {
  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    missedEventsGenerator <- MissedEventsGenerator(gitLabThrottler, executionTimeRecorder, logger)
  } yield new EventHandler[IO](categoryName, missedEventsGenerator, logger)
}
