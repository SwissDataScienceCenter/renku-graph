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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import cats.data.OptionT
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.Generators.fullCommitSyncEvents
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.Generators.commitInfos
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.{CommitInfoFinder, EventDetailsFinder}
import ch.datascience.commiteventservice.events.categories.commitsync.{categoryName, logMessageCommon}
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CommitEventSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "synchronizeEvents" should {

    "succeed if the latest eventIds in the Event Log " +
      "matches the latest commits in GitLab for relevant projects" in new TestCase {

        val event            = fullCommitSyncEvents.generateOne
        val latestCommitInfo = commitInfos.generateOne.copy(event.id)
        (accessTokenFinder
          .findAccessToken(_: Id)(_: Id => String))
          .expects(event.project.id, projectIdToPath)
          .returning(Success(maybeAccessToken))
        (latestCommitFinder.findLatestCommit _)
          .expects(event.project.id, maybeAccessToken)
          .returning(OptionT.some[Try](latestCommitInfo))

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          Info(s"${logMessageCommon(event)} -> no new events found in ${executionTimeRecorder.elapsedTime}ms")
        )

      }

    "succeed if there is a new commit and the creation succeeds for the commit and its parents" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id))
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))

      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.getEventDetails _).expects(event.project.id, latestCommitInfo.id).returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(latestCommitInfo.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(Created))

      (eventDetailsFinder.getEventDetails _).expects(event.project.id, parentCommit.id).returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parentCommit.id, maybeAccessToken)
        .returning(Success(parentCommit.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, parentCommit.id, maybeAccessToken)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${parentCommit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 2 created, 0 existed, 0 deleted, 0 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )

    }

    "succeed if there are no latest commit (project removed) and start the deletion process" in new TestCase {
      val event        = fullCommitSyncEvents.generateOne
      val parentCommit = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.none)

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, event.id)
        .returning(Success(Some(CommitWithParents(event.id, event.project.id, List(parentCommit.id)))))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, event.id, maybeAccessToken)
        .returning(Success(None))

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, event.id)
        .returning(Success(Deleted))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parentCommit.id)
        .returning(Success(Some(CommitWithParents(parentCommit.id, event.project.id, List.empty[CommitId]))))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parentCommit.id, maybeAccessToken)
        .returning(Success(None))

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, parentCommit.id)
        .returning(Success(Deleted))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Info(
          s"$categoryName: id = ${event.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events found for deletion in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${parentCommit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events found for deletion in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${event.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 0 created, 0 existed, 2 deleted, 0 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )
    }

    "succeed if there is a latest commit to create and a parent to delete" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id))
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some(latestCommitInfo))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, latestCommitInfo.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(latestCommitInfo.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(Created))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parentCommit.id)
        .returning(Success(Some(CommitWithParents(parentCommit.id, event.project.id, List.empty[CommitId]))))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parentCommit.id, maybeAccessToken)
        .returning(Success(None))

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, parentCommit.id)
        .returning(Success(Deleted))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${parentCommit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events found for deletion in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 1 created, 0 existed, 1 deleted, 0 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )
    }

    "succeed if the commit creation and deletion are not needed" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, latestCommitInfo.id)
        .returning(Success(Some(CommitWithParents(event.id, event.project.id, List.empty[CommitId]))))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(Some(commitInfos.generateOne)))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> no new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 0 created, 0 existed, 0 deleted, 0 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )
    }

    "succeed and continue the process if fetching event details fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val parent2Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id, parent2Commit.id))
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))

      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, latestCommitInfo.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(latestCommitInfo.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(Created))

      val exception = exceptions.generateOne
      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parent1Commit.id)
        .returning(Failure(exception))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parent2Commit.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parent2Commit.id, maybeAccessToken)
        .returning(Success(parent2Commit.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, parent2Commit.id, maybeAccessToken)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Info(
          s"$categoryName: id = ${parent2Commit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Error(
          s"$categoryName: id = ${parent1Commit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> Synchronization failed in ${executionTimeRecorder.elapsedTime}ms",
          exception
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 2 created, 0 existed, 0 deleted, 1 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )
    }

    "succeed and continue the process if fetching commit info fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val parent2Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id, parent2Commit.id))
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))

      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, latestCommitInfo.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(latestCommitInfo.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(Created))

      (eventDetailsFinder.getEventDetails _).expects(event.project.id, parent1Commit.id).returning(Success(None))

      val exception = exceptions.generateOne
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parent1Commit.id, maybeAccessToken)
        .returning(Failure(exception))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parent2Commit.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parent2Commit.id, maybeAccessToken)
        .returning(Success(parent2Commit.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, parent2Commit.id, maybeAccessToken)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Error(
          s"$categoryName: id = ${parent1Commit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> Synchronization failed in ${executionTimeRecorder.elapsedTime}ms",
          exception
        ),
        Info(
          s"$categoryName: id = ${parent2Commit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 2 created, 0 existed, 0 deleted, 1 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )
    }

    "succeed and continue the process if the commit creation is needed and fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id))
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))

      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, latestCommitInfo.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(latestCommitInfo.some))

      val exception = exceptions.generateOne

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(Failed(exception.getMessage, exception)))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parent1Commit.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parent1Commit.id, maybeAccessToken)
        .returning(Success(parent1Commit.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, parent1Commit.id, maybeAccessToken)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Error(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> ${exception.getMessage} in ${executionTimeRecorder.elapsedTime}ms",
          exception
        ),
        Info(
          s"$categoryName: id = ${parent1Commit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 1 created, 0 existed, 0 deleted, 1 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )
    }

    "succeed and continue the process if the commit deletion is needed and fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))

      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, latestCommitInfo.id)
        .returning(Success(Some(CommitWithParents(latestCommitInfo.id, event.project.id, List(parent1Commit.id)))))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(None))

      val exception = exceptions.generateOne

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, latestCommitInfo.id)
        .returning(Success(Failed(exception.getMessage, exception)))

      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parent1Commit.id)
        .returning(Success(None))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, parent1Commit.id, maybeAccessToken)
        .returning(Success(parent1Commit.some))

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event.project, parent1Commit.id, maybeAccessToken)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Error(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> ${exception.getMessage} in ${executionTimeRecorder.elapsedTime}ms",
          exception
        ),
        Info(
          s"$categoryName: id = ${parent1Commit.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> new events found in ${executionTimeRecorder.elapsedTime}ms"
        ),
        Info(
          s"$categoryName: id = ${latestCommitInfo.id}, projectId = ${event.project.id}, projectPath = ${event.project.path} -> events generation result: 1 created, 0 existed, 0 deleted, 1 failed in ${executionTimeRecorder.elapsedTime}ms"
        )
      )
    }

    "fail if finding Access Token for one of the event fails" in new TestCase {
      val event     = fullCommitSyncEvents.generateOne
      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Failure(exception))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Failure(exception)

      logger.loggedOnly(Error(s"${logMessageCommon(event)} -> Synchronization failed", exception))
    }
  }

  private trait TestCase {

    val logger = TestLogger[Try]()

    val maybeAccessToken   = personalAccessTokens.generateOption
    val accessTokenFinder  = mock[AccessTokenFinder[Try]]
    val latestCommitFinder = mock[LatestCommitFinder[Try]]

    val eventDetailsFinder = mock[EventDetailsFinder[Try]]
    val commitInfoFinder   = mock[CommitInfoFinder[Try]]

    val missedEventsGenerator = mock[MissedEventsGenerator[Try]]
    val commitEventsRemover   = mock[CommitEventsRemover[Try]]

    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger)

    val commitEventSynchronizer = new CommitEventSynchronizerImpl[Try](accessTokenFinder,
                                                                       latestCommitFinder,
                                                                       eventDetailsFinder,
                                                                       commitInfoFinder,
                                                                       missedEventsGenerator,
                                                                       commitEventsRemover,
                                                                       executionTimeRecorder,
                                                                       logger
    )
  }
}
