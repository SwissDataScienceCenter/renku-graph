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

package io.renku.commiteventservice.events.categories.globalcommitsync

import cats.data.EitherT.fromEither
import cats.effect.kernel.Deferred
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.Decoder
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.{CategoryName, CommitId}
import io.renku.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: Spawn: Concurrent: Logger](
    override val categoryName:  CategoryName,
    commitEventSynchronizer:    GlobalCommitEventSynchronizer[Interpretation],
    subscriptionMechanism:      SubscriptionMechanism[Interpretation],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](concurrentProcessesLimiter) {

  import commitEventSynchronizer._
  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess.withWaitingForCompletion[Interpretation](
      process = startEventProcessing(request, _),
      releaseProcess = subscriptionMechanism.renewSubscription()
    )

  private def startEventProcessing(request: EventRequestContent, deferred: Deferred[Interpretation, Unit]) =
    for {
      event <-
        fromEither[Interpretation](
          request.event.as[GlobalCommitSyncEvent].leftMap(_ => BadRequest)
        )
      result <-
        Spawn[Interpretation]
          .start {
            (synchronizeEvents(event) >> deferred.complete(())).void
              .recoverWith(finishProcessAndLogError(deferred, event))
          }
          .toRightT
          .map(_ => Accepted)
          .semiflatTap(Logger[Interpretation] log event)
          .leftSemiflatTap(Logger[Interpretation] log event)
    } yield result

  private implicit val eventDecoder: Decoder[GlobalCommitSyncEvent] = cursor =>
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

  private def finishProcessAndLogError(deferred: Deferred[Interpretation, Unit],
                                       event:    GlobalCommitSyncEvent
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    deferred.complete(()) >> Logger[Interpretation].logError(event, exception) >> exception
      .raiseError[Interpretation, Unit]
  }
}

private[events] object EventHandler {

  import eu.timepit.refined.auto._
  val processesLimit: Int Refined Positive = 1

  def apply[Interpretation[_]: Async: Spawn: Concurrent: Temporal: Logger](
      subscriptionMechanism: SubscriptionMechanism[Interpretation],
      gitLabThrottler:       Throttler[Interpretation, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
  ): Interpretation[EventHandler[Interpretation]] = for {
    concurrentProcessesLimiter    <- ConcurrentProcessesLimiter(processesLimit)
    globalCommitEventSynchronizer <- GlobalCommitEventSynchronizer(gitLabThrottler, executionTimeRecorder)
  } yield new EventHandler[Interpretation](categoryName,
                                           globalCommitEventSynchronizer,
                                           subscriptionMechanism,
                                           concurrentProcessesLimiter
  )
}
