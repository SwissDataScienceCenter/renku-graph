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

package io.renku.commiteventservice.events.categories.commitsync

import cats.MonadThrow
import cats.data.EitherT
import cats.data.EitherT.fromEither
import cats.effect.kernel.Temporal
import cats.effect.{Async, Concurrent, Spawn}
import cats.syntax.all._
import io.circe.Decoder
import io.renku.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers._
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.{CategoryName, CommitId, LastSyncedDate}
import io.renku.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: MonadThrow: Spawn: Concurrent: Logger](
    override val categoryName: CategoryName,
    commitEventSynchronizer:   CommitEventSynchronizer[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](ConcurrentProcessesLimiter.withoutLimit) {

  import commitEventSynchronizer._
  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess[Interpretation](startSynchronizingEvents(request))

  private def startSynchronizingEvents(
      request: EventRequestContent
  ): EitherT[Interpretation, EventSchedulingResult, Accepted] =
    for {
      event <-
        fromEither[Interpretation](
          request.event.as[CommitSyncEvent].leftMap(_ => BadRequest)
        )
      result <- Spawn[Interpretation]
                  .start(synchronizeEvents(event) recoverWith logError(event))
                  .toRightT
                  .map(_ => Accepted)
                  .semiflatTap(Logger[Interpretation] log event)
                  .leftSemiflatTap(Logger[Interpretation] log event)
    } yield result

  private def logError(event: CommitSyncEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      Logger[Interpretation].logError(event, exception)
      exception.raiseError[Interpretation, Unit]
  }

  private implicit val eventDecoder: Decoder[CommitSyncEvent] = cursor =>
    cursor.downField("id").as[Option[CommitId]] flatMap {
      case Some(id) =>
        for {
          project    <- cursor.downField("project").as[Project]
          lastSynced <- cursor.downField("lastSynced").as[LastSyncedDate]
        } yield FullCommitSyncEvent(id, project, lastSynced)
      case None =>
        for {
          project <- cursor.downField("project").as[Project]
        } yield MinimalCommitSyncEvent(project)
    }

  private implicit lazy val projectDecoder: Decoder[Project] = cursor =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield Project(id, path)
}

private[events] object EventHandler {
  def apply[Interpretation[_]: Async: Spawn: Concurrent: Temporal: Logger](
      gitLabThrottler:       Throttler[Interpretation, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
  ): Interpretation[EventHandler[Interpretation]] = for {
    commitEventSynchronizer <- CommitEventSynchronizer(gitLabThrottler, executionTimeRecorder)
  } yield new EventHandler[Interpretation](categoryName, commitEventSynchronizer)
}
