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

package ch.datascience.webhookservice.hookcreation

import cats.MonadError
import cats.data.OptionT
import cats.effect.IO
import cats.implicits._
import ch.datascience.clients.AccessToken
import ch.datascience.graph.events.{HookAccessToken, Project, PushUser}
import ch.datascience.logging.IOLogger
import ch.datascience.webhookservice.eventprocessing.CommitEventsOrigin
import ch.datascience.webhookservice.eventprocessing.pushevent.{IOPushEventSender, PushEventSender}
import ch.datascience.webhookservice.hookcreation.LatestPushEventFetcher.PushEventInfo
import ch.datascience.webhookservice.hookcreation.UserInfoFinder.UserInfo
import ch.datascience.webhookservice.model.ProjectInfo
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

private class EventsHistoryLoader[Interpretation[_]](
    latestPushEventFetcher: LatestPushEventFetcher[Interpretation],
    userInfoFinder:         UserInfoFinder[Interpretation],
    pushEventSender:        PushEventSender[Interpretation],
    logger:                 Logger[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable]) {

  import latestPushEventFetcher._
  import pushEventSender._
  import userInfoFinder._

  def loadAllEvents(projectInfo:     ProjectInfo,
                    hookAccessToken: HookAccessToken,
                    accessToken:     AccessToken): Interpretation[Unit] = {
    for {
      latestPushEvent    <- OptionT(fetchLatestPushEvent(projectInfo.id, accessToken))
      userInfo           <- OptionT.liftF(findUserInfo(latestPushEvent.authorId, accessToken))
      commitEventsOrigin <- OptionT.liftF(commitEventsOrigin(latestPushEvent, projectInfo, userInfo, hookAccessToken))
      _                  <- OptionT.liftF(storeCommitsInEventLog(commitEventsOrigin))
      _                  <- OptionT.liftF(logger.info(s"Project: ${projectInfo.id}: events history sent to the Event Log"))
    } yield ()
  }.value
    .flatMap(logNoEventsSent(projectInfo))
    .recoverWith(loggingError(projectInfo))

  private def commitEventsOrigin(pushEventInfo:   PushEventInfo,
                                 projectInfo:     ProjectInfo,
                                 userInfo:        UserInfo,
                                 hookAccessToken: HookAccessToken) = ME.pure {
    CommitEventsOrigin(
      maybeCommitFrom = None,
      commitTo        = pushEventInfo.commitTo,
      pushUser        = PushUser(userInfo.userId, userInfo.username, userInfo.email),
      project         = Project(projectInfo.id, projectInfo.path),
      hookAccessToken = hookAccessToken
    )
  }

  private def logNoEventsSent(projectInfo: ProjectInfo): Option[Unit] => Interpretation[Unit] = {
    case None => logger.info(s"Project: ${projectInfo.id}: No events to be sent to the Event Log")
    case _    => ME.pure(())
  }

  private def loggingError(projectInfo: ProjectInfo): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Project: ${projectInfo.id}: Sending events to the Event Log failed")
      ME.raiseError(exception)
  }
}

@Singleton
private class IOEventsHistoryLoader @Inject()(
    latestPushEventFetcher: IOLatestPushEventFetcher,
    userInfoFinder:         IOUserInfoFinder,
    pushEventSender:        IOPushEventSender,
    logger:                 IOLogger
) extends EventsHistoryLoader[IO](latestPushEventFetcher, userInfoFinder, pushEventSender, logger)
