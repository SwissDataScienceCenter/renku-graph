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

package ch.datascience.commiteventservice.events.categories.globalcommitsync

import cats.MonadThrow
import cats.data.EitherT.fromEither
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventRequestContent, EventSchedulingResult, Project}
import ch.datascience.graph.model.events.{CategoryName, CommitId}
import ch.datascience.logging.ExecutionTimeRecorder
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.Decoder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: MonadThrow: ContextShift: Concurrent](
    override val categoryName:  CategoryName,
    commitEventSynchronizer:    GlobalCommitEventSynchronizer[Interpretation],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[Interpretation],
    logger:                     Logger[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](concurrentProcessesLimiter) {
  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import commitEventSynchronizer._

  override def handle(
      request: EventRequestContent
  ): Interpretation[(Deferred[Interpretation, Unit], Interpretation[EventSchedulingResult])] =
    Deferred[Interpretation, Unit].map(done => done -> startSynchronizingEvents(request, done))

  private def startSynchronizingEvents(request: EventRequestContent, done: Deferred[Interpretation, Unit]) = {
    for {
      event <-
        fromEither[Interpretation](
          request.event.as[GlobalCommitSyncEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
        )
      result <-
        (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
          .start(
            synchronizeEvents(event)
              .flatMap(_ => done.complete(()))
              .recoverWith(finishProcessAndLogError(done, event))
          )).toRightT
          .map(_ => Accepted: EventSchedulingResult)
          .semiflatTap(logger log event)
          .leftSemiflatTap(logger log event)
    } yield result
  }.merge

  private implicit val eventDecoder: Decoder[GlobalCommitSyncEvent] =
    cursor =>
      for {
        project   <- cursor.downField("project").as[Project]
        commitIds <- cursor.downField("commits").as[List[CommitId]]
      } yield GlobalCommitSyncEvent(project, commitIds)

  private implicit lazy val projectDecoder: Decoder[Project] = cursor =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield Project(id, path)

  private implicit lazy val eventInfoToString: GlobalCommitSyncEvent => String = _.toString

  private def finishProcessAndLogError(done:  Deferred[Interpretation, Unit],
                                       event: GlobalCommitSyncEvent
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    done.complete(()) >> logger.logError(event, exception) >> exception.raiseError[Interpretation, Unit]
  }
}

private[events] object EventHandler {

  import eu.timepit.refined.auto._
  val processesLimit: Int Refined Positive = 1

  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    concurrentProcessesLimiter    <- ConcurrentProcessesLimiter(processesLimit)
    globalCommitEventSynchronizer <- GlobalCommitEventSynchronizer(gitLabThrottler, executionTimeRecorder, logger)
  } yield new EventHandler[IO](categoryName, globalCommitEventSynchronizer, concurrentProcessesLimiter, logger)
}
