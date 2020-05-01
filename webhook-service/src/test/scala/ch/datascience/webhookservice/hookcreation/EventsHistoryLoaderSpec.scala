/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.webhookservice.commits.{CommitInfo, LatestCommitFinder}
import ch.datascience.webhookservice.eventprocessing.startcommit.TryCommitToEventLog
import ch.datascience.webhookservice.eventprocessing.{Project, StartCommit}
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.project.ProjectInfo
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Success, Try}

class EventsHistoryLoaderSpec extends WordSpec with MockFactory {

  "loadAllEvents" should {

    "fetch latest Commit and other missing bits of info, build start Commit and pass it to the CommitToEventLog" in new TestCase {

      val commitInfo = commitInfos.generateOne
      (latestCommitFinder
        .findLatestCommit(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(OptionT.some[Try](commitInfo))

      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
        .expects(startCommitFrom(commitInfo, projectInfo))
        .returning(context.pure(()))

      eventsHistoryLoader.loadAllEvents(projectInfo, accessToken) shouldBe Success(())
    }

    "do nothing if there's no latest Commit" in new TestCase {

      (latestCommitFinder
        .findLatestCommit(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(OptionT.none[Try, CommitInfo])

      eventsHistoryLoader.loadAllEvents(projectInfo, accessToken) shouldBe Success(())
    }

    "fail if fetching latest Commit fails" in new TestCase {

      val exception = exceptions.generateOne
      val error     = context.raiseError(exception)
      (latestCommitFinder
        .findLatestCommit(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(OptionT.liftF(error))

      eventsHistoryLoader.loadAllEvents(projectInfo, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Project: ${projectInfo.id}: Sending events to the Event Log failed", exception))
    }

    "fail if sending start Commit to the Event Log fails" in new TestCase {

      val commitInfo = commitInfos.generateOne
      (latestCommitFinder
        .findLatestCommit(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(OptionT.some[Try](commitInfo))

      val exception = exceptions.generateOne
      val error     = context.raiseError(exception)
      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
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

    val latestCommitFinder = mock[LatestCommitFinder[Try]]
    val commitToEventLog   = mock[TryCommitToEventLog]
    val logger             = TestLogger[Try]()
    val eventsHistoryLoader = new EventsHistoryLoader[Try](
      latestCommitFinder,
      commitToEventLog,
      logger
    )
  }

  private def startCommitFrom(commitInfo: CommitInfo, projectInfo: ProjectInfo) = StartCommit(
    id      = commitInfo.id,
    project = Project(projectInfo.id, projectInfo.path)
  )
}
