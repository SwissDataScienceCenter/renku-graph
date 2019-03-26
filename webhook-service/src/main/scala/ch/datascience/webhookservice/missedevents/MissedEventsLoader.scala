/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.missedevents

import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.dbeventlog.commands.EventLogLatestEvents
import ch.datascience.graph.model.events.{CommitEventId, Project}
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.pushevent.PushEventSender
import ch.datascience.webhookservice.project.{ProjectInfo, ProjectInfoFinder}
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher.PushEventInfo
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds
import scala.util.control.NonFatal

private abstract class MissedEventsLoader[Interpretation[_]] {
  def loadMissedEvents: Interpretation[Unit]
}

private class IOMissedEventsLoader(
    eventLogLatestEvents:   EventLogLatestEvents[IO],
    accessTokenFinder:      AccessTokenFinder[IO],
    latestPushEventFetcher: LatestPushEventFetcher[IO],
    projectInfoFinder:      ProjectInfoFinder[IO],
    pushEventSender:        PushEventSender[IO],
    logger:                 Logger[IO],
    executionTimeRecorder:  ExecutionTimeRecorder[IO]
)(implicit contextShift:    ContextShift[IO])
    extends MissedEventsLoader[IO] {

  import UpdateResult._
  import accessTokenFinder._
  import eventLogLatestEvents._
  import executionTimeRecorder._
  import latestPushEventFetcher._
  import projectInfoFinder._
  import pushEventSender._

  def loadMissedEvents: IO[Unit] = {
    measureExecutionTime {
      for {
        latestLogEvents <- findAllLatestEvents
        updateSummary <- if (latestLogEvents.isEmpty) IO.pure(UpdateSummary())
                        else (latestLogEvents map loadEvents).parSequence.map(toUpdateSummary)
      } yield updateSummary
    } flatMap logSummary
  } recoverWith loggingError

  private lazy val logSummary: ((ElapsedTime, UpdateSummary)) => IO[Unit] = {
    case (elapsedTime, updateSummary) =>
      logger.info(
        s"Synchronized Push Events with GitLab in ${elapsedTime}ms: " +
          s"${updateSummary(Updated)} updates, " +
          s"${updateSummary(Skipped)} skipped, " +
          s"${updateSummary(Failed)} failed"
      )
  }

  private def loadEvents(latestLogEvent: CommitEventId): IO[UpdateResult] = {
    for {
      maybeAccessToken   <- findAccessToken(latestLogEvent.projectId)
      maybePushEventInfo <- fetchLatestPushEvent(latestLogEvent.projectId, maybeAccessToken)
      updateResult       <- addEventsIfMissing(latestLogEvent, maybePushEventInfo, maybeAccessToken)
    } yield updateResult
  } recoverWith loggingWarning(latestLogEvent)

  private def addEventsIfMissing(latestLogEvent:     CommitEventId,
                                 maybePushEventInfo: Option[PushEventInfo],
                                 maybeAccessToken:   Option[AccessToken]) =
    maybePushEventInfo match {
      case None                                         => IO.pure(Skipped)
      case Some(PushEventInfo(_, _, latestLogEvent.id)) => IO.pure(Skipped)
      case Some(pushEventInfo) =>
        for {
          projectInfo <- findProjectInfo(latestLogEvent.projectId, maybeAccessToken)
          pushEvent   <- constructPushEvent(pushEventInfo, projectInfo)
          _           <- storeCommitsInEventLog(pushEvent)
        } yield Updated
    }

  private def constructPushEvent(pushEventInfo: PushEventInfo, projectInfo: ProjectInfo) = IO.pure {
    PushEvent(
      maybeCommitFrom = None,
      commitTo        = pushEventInfo.commitTo,
      pushUser        = pushEventInfo.pushUser,
      project         = Project(projectInfo.id, projectInfo.path)
    )
  }

  private def loggingWarning(latestLogEvent: CommitEventId): PartialFunction[Throwable, IO[UpdateResult]] = {
    case NonFatal(exception) =>
      logger.warn(exception)(s"Synchronizing Push Events for project ${latestLogEvent.projectId} failed")
      IO.pure(Failed)
  }

  private lazy val loggingError: PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Synchronizing Push Events with GitLab failed")
      IO.raiseError(exception)
  }

  private sealed trait UpdateResult extends Product with Serializable
  private object UpdateResult {
    final case object Skipped extends UpdateResult
    final case object Updated extends UpdateResult
    final case object Failed  extends UpdateResult
  }

  private case class UpdateSummary(state: Map[UpdateResult, Int] = Map.empty) {
    def apply(updateResult: UpdateResult): Int = state.getOrElse(updateResult, 0)
  }

  private def toUpdateSummary(updateResults: List[UpdateResult]) = UpdateSummary(
    updateResults.groupBy(identity).mapValues(_.size)
  )
}
