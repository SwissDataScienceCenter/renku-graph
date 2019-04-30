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
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events._
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.pushevent.TryPushEventSender
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.project.ProjectInfo
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher.PushEventInfo
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Success, Try}

class EventsHistoryLoaderSpec extends WordSpec with MockFactory {

  "loadAllEvents" should {

    "fetch latest push event and other missing bits of info, build Push Event and pass it to the Push Event Sender" in new TestCase {

      val pushEventInfo = pushEventInfoFrom(projectInfo)
      (latestPushEventFetcher
        .fetchLatestPushEvent(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(Some(pushEventInfo)))

      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(pushEventFrom(pushEventInfo, projectInfo))
        .returning(context.pure(()))

      eventsHistoryLoader.loadAllEvents(projectInfo, accessToken) shouldBe Success(())
    }

    "do nothing if there's no latest push event" in new TestCase {

      val pushEventInfo = pushEventInfoFrom(projectInfo)
      (latestPushEventFetcher
        .fetchLatestPushEvent(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(None))

      eventsHistoryLoader.loadAllEvents(projectInfo, accessToken) shouldBe Success(())
    }

    "fail if fetching latest push event fails" in new TestCase {

      val exception = exceptions.generateOne
      val error     = context.raiseError(exception)
      (latestPushEventFetcher
        .fetchLatestPushEvent(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(error)

      eventsHistoryLoader.loadAllEvents(projectInfo, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Project: ${projectInfo.id}: Sending events to the Event Log failed", exception))
    }

    "fail if sending push event to the Event Log fails" in new TestCase {

      val pushEventInfo = pushEventInfoFrom(projectInfo)
      (latestPushEventFetcher
        .fetchLatestPushEvent(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(Some(pushEventInfo)))

      val exception = exceptions.generateOne
      val error     = context.raiseError(exception)
      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(*)
        .returning(error)

      eventsHistoryLoader.loadAllEvents(projectInfo, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Project: ${projectInfo.id}: Sending events to the Event Log failed", exception))
    }
  }

  private trait TestCase {
    val projectInfo = projectInfos.generateOne
    val projectId   = projectInfo.id
    val accessToken = accessTokens.generateOne

    val context = MonadError[Try, Throwable]

    val latestPushEventFetcher = mock[LatestPushEventFetcher[Try]]
    val pushEventSender        = mock[TryPushEventSender]
    val logger                 = TestLogger[Try]()
    val eventsHistoryLoader = new EventsHistoryLoader[Try](
      latestPushEventFetcher,
      pushEventSender,
      logger
    )
  }

  private def pushEventInfoFrom(projectInfo: ProjectInfo) =
    pushEventInfos.generateOne
      .copy(projectId = projectInfo.id)

  private def pushEventFrom(pushEventInfo: PushEventInfo, projectInfo: ProjectInfo) =
    PushEvent(
      maybeCommitFrom = None,
      commitTo        = pushEventInfo.commitTo,
      pushUser        = pushEventInfo.pushUser,
      project         = Project(projectInfo.id, projectInfo.path)
    )
}
