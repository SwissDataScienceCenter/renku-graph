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
import ch.datascience.commiteventservice.events.categories.commitsync.Generators._
import ch.datascience.commiteventservice.events.categories.commitsync.{categoryName, logMessageCommon}
import ch.datascience.commiteventservice.events.categories.common.Generators.commitInfos
import ch.datascience.commiteventservice.events.categories.common.UpdateResult._
import ch.datascience.commiteventservice.events.categories.common._
import ch.datascience.events.consumers.Project
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{batchDates, commitIds}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder._
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, ZoneId, ZoneOffset}
import scala.util.{Failure, Success, Try}

class CommitEventSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "synchronizeEvents" should {

    "succeed if the latest eventIds in the Event Log " +
      "matches the latest commits in GitLab for relevant projects" in new TestCase {

        val event            = fullCommitSyncEvents.generateOne
        val latestCommitInfo = commitInfos.generateOne.copy(event.id)

        givenAccessTokenIsFound(event.project.id)

        givenLatestCommitIsFound(latestCommitInfo, event.project.id)

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          Info(s"${logMessageCommon(event)} -> event skipped in ${executionTimeRecorder.elapsedTime}ms")
        )
      }

    "succeed if the latest event in the Event Log  is already in EventLog and log the event as Existed" in new TestCase {

      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne.copy(parents = commitIds.generateNonEmptyList().toList)

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsInEL(latestCommitInfo.id, event.project.id)(
        CommitWithParents(latestCommitInfo.id, event.project.id, parents = commitIds.generateNonEmptyList().toList)
      )

      givenCommitIsInGL(latestCommitInfo, event.project.id)

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logNoNewEvents(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, existed = 1)
      )

    }

    "succeed if the latest event in the Event Log has the id '0000000000000000000000000000000000000000'" +
      "log the event as skipped" in new TestCase {

        val event = fullCommitSyncEvents.generateOne
        val latestCommitInfo = commitInfos.generateOne.copy(id = CommitId("0000000000000000000000000000000000000000"),
                                                            parents = List.empty[CommitId]
        )

        givenAccessTokenIsFound(event.project.id)

        givenLatestCommitIsFound(latestCommitInfo, event.project.id)

        givenEventIsNotInEL(latestCommitInfo, event.project.id)
        givenCommitIsInGL(latestCommitInfo, event.project.id)

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          logEventSkipped(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, skipped = 1)
        )

      }

    "succeed if there is a new commit and the creation succeeds for the first commit and stops once ids are on both gitlab and EL" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id, commitIds.generateOne))

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(Success(Created))

      givenEventIsInEL(parentCommit.id, event.project.id)(
        CommitWithParents(parentCommit.id, event.project.id, parentCommit.parents)
      )
      givenCommitIsInGL(parentCommit, event.project.id)

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logNoNewEvents(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 1, existed = 1)
      )

    }

    "succeed if there is a new commit and the creation succeeds for the commit and its parents" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id))

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(Success(Created))

      givenEventIsNotInEL(parentCommit, event.project.id)
      givenCommitIsInGL(parentCommit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parentCommit, batchDate)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logNewEventFound(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 2)
      )

    }

    "succeed if there are no latest commit (project removed) and start the deletion process" in new TestCase {
      val event        = fullCommitSyncEvents.generateOne
      val parentCommit = commitInfos.generateOne.copy(parents = List.empty[CommitId])

      givenAccessTokenIsFound(event.project.id)

      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.none)

      givenEventIsInEL(event.id, event.project.id)(returning =
        CommitWithParents(event.id, event.project.id, List(parentCommit.id))
      )
      givenCommitIsNotInGL(event.id, event.project.id)

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, event.id)
        .returning(UpdateResult.Deleted.pure[Try])

      givenEventIsInEL(parentCommit.id, event.project.id)(returning =
        CommitWithParents(parentCommit.id, event.project.id, List.empty[CommitId])
      )
      givenCommitIsNotInGL(parentCommit.id, event.project.id)

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, parentCommit.id)
        .returning(UpdateResult.Deleted.pure[Try])

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logEventFoundForDeletion(event.id, event.project, executionTimeRecorder.elapsedTime),
        logEventFoundForDeletion(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(event.id, event.project, executionTimeRecorder.elapsedTime, deleted = 2)
      )
    }

    "succeed if there is a latest commit to create and a parent to delete" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id))

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(Success(Created))

      givenEventIsInEL(parentCommit.id, event.project.id)(returning =
        CommitWithParents(parentCommit.id, event.project.id, List.empty[CommitId])
      )
      givenCommitIsNotInGL(parentCommit.id, event.project.id)

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, parentCommit.id)
        .returning(UpdateResult.Deleted.pure[Try])

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logEventFoundForDeletion(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 1, deleted = 1)
      )
    }

    "succeed and continue the process if fetching event details fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val parent2Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id, parent2Commit.id))

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(Success(Created))

      val exception = exceptions.generateOne
      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parent1Commit.id)
        .returning(Failure(exception))

      givenEventIsNotInEL(parent2Commit, event.project.id)
      givenCommitIsInGL(parent2Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent2Commit, batchDate)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logNewEventFound(parent2Commit.id, event.project, executionTimeRecorder.elapsedTime),
        logErrorSynchronization(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime, exception),
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 2, failed = 1)
      )
    }

    "succeed and continue the process if fetching commit info fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val parent2Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id, parent2Commit.id))

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(Success(Created))

      givenEventIsNotInEL(parent1Commit, event.project.id)

      val exception = exceptions.generateOne
      (commitInfoFinder
        .getMaybeCommitInfo(_: Id, _: CommitId)(_: Option[AccessToken]))
        .expects(event.project.id, parent1Commit.id, maybeAccessToken)
        .returning(Failure(exception))

      givenEventIsNotInEL(parent2Commit, event.project.id)
      givenCommitIsInGL(parent2Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent2Commit, batchDate)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logErrorSynchronization(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime, exception),
        logNewEventFound(parent2Commit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 2, failed = 1)
      )
    }

    "succeed and continue the process if the commit creation is needed and fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id))

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      val exception = exceptions.generateOne

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(Success(Failed(exception.getMessage, exception)))

      givenEventIsNotInEL(parent1Commit, event.project.id)
      givenCommitIsInGL(parent1Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent1Commit, batchDate)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logErrorSynchronization(latestCommitInfo.id,
                                event.project,
                                executionTimeRecorder.elapsedTime,
                                exception,
                                exception.getMessage
        ),
        logNewEventFound(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 1, failed = 1)
      )
    }

    "succeed and continue the process if the commit deletion is needed and return a failure" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsInEL(latestCommitInfo.id, event.project.id)(
        CommitWithParents(latestCommitInfo.id, event.project.id, List(parent1Commit.id))
      )
      givenCommitIsNotInGL(latestCommitInfo.id, event.project.id)

      val exception       = exceptions.generateOne
      val deletionFailure = UpdateResult.Failed(nonEmptyStrings().generateOne, exception)
      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, latestCommitInfo.id)
        .returning(deletionFailure.pure[Try])

      givenEventIsNotInEL(parent1Commit, event.project.id)
      givenCommitIsInGL(parent1Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent1Commit, batchDate)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logErrorSynchronization(latestCommitInfo.id,
                                event.project,
                                executionTimeRecorder.elapsedTime,
                                exception,
                                deletionFailure.message
        ),
        logNewEventFound(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 1, failed = 1)
      )
    }

    "succeed and continue the process if the commit deletion is needed and fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsInEL(latestCommitInfo.id, event.project.id)(
        CommitWithParents(latestCommitInfo.id, event.project.id, List(parent1Commit.id))
      )
      givenCommitIsNotInGL(latestCommitInfo.id, event.project.id)

      val exception = exceptions.generateOne
      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, latestCommitInfo.id)
        .returning(exception.raiseError[Try, UpdateResult])

      givenEventIsNotInEL(parent1Commit, event.project.id)
      givenCommitIsInGL(parent1Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent1Commit, batchDate)
        .returning(Success(Created))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logErrorSynchronization(latestCommitInfo.id,
                                event.project,
                                executionTimeRecorder.elapsedTime,
                                exception,
                                "COMMIT_SYNC - Commit Remover failed to send commit deletion status"
        ),
        logNewEventFound(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime, created = 1, failed = 1)
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

    val maybeAccessToken = personalAccessTokens.generateOption
    val batchDate        = batchDates.generateOne

    val clock = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))

    val accessTokenFinder  = mock[AccessTokenFinder[Try]]
    val latestCommitFinder = mock[LatestCommitFinder[Try]]

    val eventDetailsFinder = mock[EventDetailsFinder[Try]]
    val commitInfoFinder   = mock[CommitInfoFinder[Try]]

    val commitToEventLog    = mock[CommitToEventLog[Try]]
    val commitEventsRemover = mock[CommitEventsRemover[Try]]

    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger)

    val commitEventSynchronizer = new CommitEventSynchronizerImpl[Try](accessTokenFinder,
                                                                       latestCommitFinder,
                                                                       eventDetailsFinder,
                                                                       commitInfoFinder,
                                                                       commitToEventLog,
                                                                       commitEventsRemover,
                                                                       executionTimeRecorder,
                                                                       logger,
                                                                       clock
    )

    def givenAccessTokenIsFound(projectId: Id) = (accessTokenFinder
      .findAccessToken(_: Id)(_: Id => String))
      .expects(projectId, projectIdToPath)
      .returning(Success(maybeAccessToken))

    def givenLatestCommitIsFound(commitInfo: CommitInfo, projectId: Id) = (latestCommitFinder.findLatestCommit _)
      .expects(projectId, maybeAccessToken)
      .returning(OptionT.some[Try](commitInfo))

    def givenCommitIsInGL(commitInfo: CommitInfo, projectId: Id) = (commitInfoFinder
      .getMaybeCommitInfo(_: Id, _: CommitId)(_: Option[AccessToken]))
      .expects(projectId, commitInfo.id, maybeAccessToken)
      .returning(Success(commitInfo.some))

    def givenEventIsInEL(commitId: CommitId, projectId: Id)(returning: CommitWithParents) =
      (eventDetailsFinder.getEventDetails _)
        .expects(projectId, commitId)
        .returning(Some(returning).pure[Try])

    def givenEventIsNotInEL(commitInfo: CommitInfo, projectId: Id) =
      (eventDetailsFinder.getEventDetails _).expects(projectId, commitInfo.id).returning(Success(None))

    def givenCommitIsNotInGL(commitId: CommitId, projectId: Id) =
      (commitInfoFinder
        .getMaybeCommitInfo(_: Id, _: CommitId)(_: Option[AccessToken]))
        .expects(projectId, commitId, maybeAccessToken)
        .returning(Success(None))
  }

  private def logSummary(commitId:    CommitId,
                         project:     Project,
                         elapsedTime: ElapsedTime,
                         created:     Int = 0,
                         existed:     Int = 0,
                         skipped:     Int = 0,
                         deleted:     Int = 0,
                         failed:      Int = 0
  ) =
    Info(
      s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> events generation result: $created created, $existed existed, $skipped skipped, $deleted deleted, $failed failed in ${elapsedTime}ms"
    )

  private def logNewEventFound(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> new events found in ${elapsedTime}ms"
  )
  private def logEventFoundForDeletion(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> events found for deletion in ${elapsedTime}ms"
  )

  private def logNoNewEvents(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> no new events found in ${elapsedTime}ms"
  )

  private def logEventSkipped(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> event skipped in ${elapsedTime}ms"
  )

  private def logErrorSynchronization(commitId:    CommitId,
                                      project:     Project,
                                      elapsedTime: ElapsedTime,
                                      exception:   Exception,
                                      message:     String = "Synchronization failed"
  ) = Error(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> $message in ${elapsedTime}ms",
    exception
  )
}
