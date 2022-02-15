/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.NonEmptyParallel
import cats.data.EitherT.fromEither
import cats.effect._
import cats.effect.kernel.Deferred
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.Decoder
import io.renku.commiteventservice.events.categories.globalcommitsync.GlobalCommitSyncEvent.CommitsInfo
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.CommitsSynchronizer
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.{CategoryName, CommitId}
import io.renku.http.client.GitLabClient
import io.renku.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[events] class EventHandler[F[_]: Spawn: Concurrent: Logger](
    override val categoryName:  CategoryName,
    commitEventSynchronizer:    CommitsSynchronizer[F],
    subscriptionMechanism:      SubscriptionMechanism[F],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F]
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  import commitEventSynchronizer._
  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess.withWaitingForCompletion[F](
      process = startEventProcessing(request, _),
      releaseProcess = subscriptionMechanism.renewSubscription()
    )

  private def startEventProcessing(request: EventRequestContent, process: Deferred[F, Unit]) = for {
    event <- fromEither[F](request.event.as[GlobalCommitSyncEvent].leftMap(_ => BadRequest))
    result <-
      Spawn[F]
        .start {
          (synchronizeEvents(event) >> process.complete(())).void
            .recoverWith(finishProcessAndLogError(process, event))
        }
        .toRightT
        .map(_ => Accepted)
        .semiflatTap(Logger[F] log event)
        .leftSemiflatTap(Logger[F] log event)
  } yield result

  private implicit val eventDecoder: Decoder[GlobalCommitSyncEvent] = cursor =>
    for {
      project        <- cursor.downField("project").as[Project]
      commitsCount   <- cursor.downField("commits").downField("count").as[CommitsCount]
      latestCommitId <- cursor.downField("commits").downField("latest").as[CommitId]
    } yield GlobalCommitSyncEvent(project, CommitsInfo(commitsCount, latestCommitId))

  private implicit lazy val projectDecoder: Decoder[Project] = cursor =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield Project(id, path)

  private implicit lazy val eventInfoToString: GlobalCommitSyncEvent => String = _.show

  private def finishProcessAndLogError(deferred: Deferred[F, Unit],
                                       event:    GlobalCommitSyncEvent
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    deferred.complete(()) >> Logger[F].logError(event, exception) >> exception.raiseError[F, Unit]
  }
}

private[events] object EventHandler {

  import eu.timepit.refined.auto._
  val processesLimit: Int Refined Positive = 1

  def apply[F[_]: Async: NonEmptyParallel: Logger](
      subscriptionMechanism: SubscriptionMechanism[F],
      gitLabClient:          GitLabClient[F],
      executionTimeRecorder: ExecutionTimeRecorder[F]
  ): F[EventHandler[F]] = for {
    concurrentProcessesLimiter    <- ConcurrentProcessesLimiter(processesLimit)
    globalCommitEventSynchronizer <- CommitsSynchronizer(gitLabClient, executionTimeRecorder)
  } yield new EventHandler[F](categoryName,
                              globalCommitEventSynchronizer,
                              subscriptionMechanism,
                              concurrentProcessesLimiter
  )
}
