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

import java.time.{Duration, Instant}

import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.dbeventlog.commands.EventLogLatestEvents
import ch.datascience.dbeventlog.commands.EventLogLatestEvents.LatestEvent
import ch.datascience.graph.model.events.Project
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.pushevent.PushEventSender
import ch.datascience.webhookservice.project.{ProjectInfo, ProjectInfoFinder}
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher.PushEventInfo
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds
import scala.util.control.NonFatal

abstract class MissedEventsLoader[Interpretation[_]] {
  def loadMissedEvents: Interpretation[Unit]
}

class IOMissedEventsLoader(
    eventLogLatestEvents:   EventLogLatestEvents[IO],
    accessTokenFinder:      AccessTokenFinder[IO],
    latestPushEventFetcher: LatestPushEventFetcher[IO],
    projectInfoFinder:      ProjectInfoFinder[IO],
    pushEventSender:        PushEventSender[IO],
    logger:                 Logger[IO],
    now:                    () => Instant = Instant.now
)(implicit contextShift:    ContextShift[IO])
    extends MissedEventsLoader[IO] {

  import accessTokenFinder._
  import eventLogLatestEvents._
  import latestPushEventFetcher._
  import projectInfoFinder._
  import pushEventSender._

  def loadMissedEvents: IO[Unit] = {
    measureExecutionTime {
      for {
        latestLogEvents <- findAllLatestEvents
        _               <- if (latestLogEvents.isEmpty) IO.unit else (latestLogEvents map loadEvents).parSequence
      } yield latestLogEvents.size
    } flatMap logSummary
  } recoverWith loggingError

  private lazy val logSummary: ((Instant, Instant, Int)) => IO[Unit] = {
    case (startTime, endTime, processedEvents) =>
      logger.info(
        s"Synchronized $processedEvents events with GitLab in ${Duration.between(startTime, endTime).getSeconds}s"
      )
  }

  private def loadEvents(latestLogEvent: LatestEvent) = {
    for {
      maybeAccessToken   <- findAccessToken(latestLogEvent.projectId)
      maybePushEventInfo <- fetchLatestPushEvent(latestLogEvent.projectId, maybeAccessToken)
      _                  <- addEventsIfMissing(latestLogEvent, maybePushEventInfo, maybeAccessToken)
    } yield ()
  } recoverWith loggingWarning(latestLogEvent)

  private def addEventsIfMissing(latestLogEvent:     LatestEvent,
                                 maybePushEventInfo: Option[PushEventInfo],
                                 maybeAccessToken:   Option[AccessToken]) =
    maybePushEventInfo match {
      case None                                              => IO.unit
      case Some(PushEventInfo(_, _, latestLogEvent.eventId)) => IO.unit
      case Some(pushEventInfo) =>
        for {
          projectInfo <- findProjectInfo(latestLogEvent.projectId, maybeAccessToken)
          pushEvent   <- constructPushEvent(pushEventInfo, projectInfo)
          _           <- storeCommitsInEventLog(pushEvent)
        } yield ()
    }

  private def constructPushEvent(pushEventInfo: PushEventInfo, projectInfo: ProjectInfo) = IO.pure {
    PushEvent(
      maybeCommitFrom = None,
      commitTo        = pushEventInfo.commitTo,
      pushUser        = pushEventInfo.pushUser,
      project         = Project(projectInfo.id, projectInfo.path)
    )
  }

  private def loggingWarning(latestLogEvent: LatestEvent): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.warn(exception)(s"Synchronizing events for project ${latestLogEvent.projectId} failed")
  }

  private lazy val loggingError: PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Synchronizing events with GitLab failed")
      IO.raiseError(exception)
  }

  private def measureExecutionTime(function: => IO[Int]): IO[(Instant, Instant, Int)] =
    for {
      startTime <- IO.pure(now())
      result    <- function
      endTime   <- IO.pure(now())
    } yield (startTime, endTime, result)
}
