/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.consumers.commitsync.eventgeneration

import cats.syntax.all._
import io.circe.literal._
import io.renku.commiteventservice.events.consumers.commitsync.{categoryName, logMessageCommon}
import io.renku.commiteventservice.events.consumers.commitsync.Generators._
import io.renku.commiteventservice.events.consumers.common._
import io.renku.commiteventservice.events.consumers.common.Generators.commitInfos
import io.renku.commiteventservice.events.consumers.common.UpdateResult._
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.{batchDates, commitIds}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits._
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level._
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, ZoneId, ZoneOffset}
import scala.util.{Failure, Success, Try}

class CommitsSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "synchronizeEvents" should {

    "succeed if the latest eventIds in the Event Log " +
      "matches the latest commits in GitLab for relevant projects" in new TestCase {

        val event            = fullCommitSyncEvents.generateOne
        val latestCommitInfo = commitInfos.generateOne.copy(event.id)

        givenAccessTokenIsFound(event.project.id)

        givenLatestCommitIsFound(latestCommitInfo, event.project.id)
        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.loggedOnly(Info(show"$categoryName: $event accepted"))
      }

    "succeed if the latest event in the Event Log is already in EventLog and log the event as Existed" in new TestCase {

      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne.copy(parents = commitIds.generateNonEmptyList().toList)

      givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsInEL(latestCommitInfo.id, event.project.id)(
        CommitWithParents(latestCommitInfo.id, event.project.id, parents = commitIds.generateNonEmptyList().toList)
      )

      givenCommitIsInGL(latestCommitInfo, event.project.id)

      commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

      logger.loggedOnly(
        Info(show"$categoryName: $event accepted"),
        logSummary(latestCommitInfo.id,
                   event.project,
                   executionTimeRecorder.elapsedTime,
                   SynchronizationSummary().updated(Existed, 1)
        )
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

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Skipped.name -> 1)
          )
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
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

      logger.logged(
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id,
                   event.project,
                   executionTimeRecorder.elapsedTime,
                   SynchronizationSummary(Existed.name -> 1, Created.name -> 1)
        )
      )

    }

    "succeed if there is a new commit and the creation succeeds for the commit and its parents " +
      "and trigger the global commit sync process" in new TestCase {
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
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logNewEventFound(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Created.name -> 2)
          )
        )
      }

    "succeed if there are no latest commit (project removed) and start the deletion process " +
      "and trigger the global commit sync process" in new TestCase {
        val event        = fullCommitSyncEvents.generateOne
        val parentCommit = commitInfos.generateOne.copy(parents = List.empty[CommitId])

        givenAccessTokenIsFound(event.project.id)

        (latestCommitFinder
          .findLatestCommit(_: projects.GitLabId)(_: Option[AccessToken]))
          .expects(event.project.id, maybeAccessToken)
          .returning(Option.empty[CommitInfo].pure[Try])

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
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logEventFoundForDeletion(event.id, event.project, executionTimeRecorder.elapsedTime),
          logEventFoundForDeletion(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(event.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Deleted.name -> 2)
          )
        )
      }

    "succeed if there is a latest commit to create and a parent to delete " +
      "and trigger the global commit sync process" in new TestCase {
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
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logEventFoundForDeletion(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Deleted.name -> 1, Created.name -> 1)
          )
        )
      }

    "succeed and continue the process if fetching event details fails " +
      "and trigger the global commit sync process" in new TestCase {
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
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logNewEventFound(parent2Commit.id, event.project, executionTimeRecorder.elapsedTime),
          logErrorSynchronization(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime, exception),
          logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Failed.name -> 1, Created.name -> 2)
          )
        )
      }

    "succeed and continue the process if fetching commit info fails " +
      "and trigger the global commit sync process" in new TestCase {
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
          .getMaybeCommitInfo(_: GitLabId, _: CommitId)(_: Option[AccessToken]))
          .expects(event.project.id, parent1Commit.id, maybeAccessToken)
          .returning(Failure(exception))

        givenEventIsNotInEL(parent2Commit, event.project.id)
        givenCommitIsInGL(parent2Commit, event.project.id)

        (commitToEventLog.storeCommitInEventLog _)
          .expects(event.project, parent2Commit, batchDate)
          .returning(Success(Created))
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logErrorSynchronization(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime, exception),
          logNewEventFound(parent2Commit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Failed.name -> 1, Created.name -> 2)
          )
        )
      }

    "succeed and continue the process if the commit creation is needed and fails " +
      "and trigger the global commit sync process" in new TestCase {
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
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logErrorSynchronization(latestCommitInfo.id,
                                  event.project,
                                  executionTimeRecorder.elapsedTime,
                                  exception,
                                  exception.getMessage
          ),
          logNewEventFound(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Failed.name -> 1, Created.name -> 1)
          )
        )
      }

    "succeed and continue the process if the commit deletion is needed and return a failure " +
      "and trigger the global commit sync process" in new TestCase {
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
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logErrorSynchronization(latestCommitInfo.id,
                                  event.project,
                                  executionTimeRecorder.elapsedTime,
                                  exception,
                                  deletionFailure.message
          ),
          logNewEventFound(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Failed.name -> 1, Created.name -> 1)
          )
        )
      }

    "succeed and continue the process if the commit deletion is needed and fails " +
      "and trigger the global commit sync process" in new TestCase {
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
        givenGlobalCommitSyncRequestIsSent(event.project)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.logged(
          logErrorSynchronization(latestCommitInfo.id,
                                  event.project,
                                  executionTimeRecorder.elapsedTime,
                                  exception,
                                  "COMMIT_SYNC: Commit Remover failed to send commit deletion status"
          ),
          logNewEventFound(parent1Commit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Failed.name -> 1, Created.name -> 1)
          )
        )
      }

    "succeed if the process is successful but the triggering of the global commit sync fails" in new TestCase {

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

      val exception = exceptions.generateOne
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          withRequestContent(event.project),
          EventSender.EventContext(CategoryName("GLOBAL_COMMIT_SYNC_REQUEST"),
                                   s"$categoryName: Triggering Global Commit Sync Failed"
          )
        )
        .returning(exception.raiseError[Try, Unit])

      commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

      logger.logged(
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id,
                   event.project,
                   executionTimeRecorder.elapsedTime,
                   SynchronizationSummary(Existed.name -> 1, Created.name -> 1)
        ),
        Error(s"${logMessageCommon(event)} -> Triggering Global Commit Sync Failed", exception)
      )

    }
    "fail if finding Access Token for one of the event fails" in new TestCase {
      val event     = fullCommitSyncEvents.generateOne
      val exception = exceptions.generateOne

      (accessTokenFinder
        .findAccessToken(_: GitLabId)(_: GitLabId => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Failure(exception))

      commitsSynchronizer.synchronizeEvents(event) shouldBe Failure(exception)

      logger.logged(Error(s"${logMessageCommon(event)} -> Synchronization failed", exception))
    }
  }

  private trait TestCase {

    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val batchDate = batchDates.generateOne

    implicit val logger:                TestLogger[Try]                = TestLogger[Try]()
    implicit val executionTimeRecorder: TestExecutionTimeRecorder[Try] = TestExecutionTimeRecorder[Try]()
    implicit val accessTokenFinder:     AccessTokenFinder[Try]         = mock[AccessTokenFinder[Try]]
    val latestCommitFinder  = mock[LatestCommitFinder[Try]]
    val eventDetailsFinder  = mock[EventDetailsFinder[Try]]
    val commitInfoFinder    = mock[CommitInfoFinder[Try]]
    val commitToEventLog    = mock[CommitToEventLog[Try]]
    val commitEventsRemover = mock[CommitEventsRemover[Try]]
    val eventSender         = mock[EventSender[Try]]
    private val clock       = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))

    val commitsSynchronizer = new CommitsSynchronizerImpl[Try](latestCommitFinder,
                                                               eventDetailsFinder,
                                                               commitInfoFinder,
                                                               commitToEventLog,
                                                               commitEventsRemover,
                                                               eventSender,
                                                               clock
    )

    def givenAccessTokenIsFound(projectId: GitLabId) = (accessTokenFinder
      .findAccessToken(_: GitLabId)(_: GitLabId => String))
      .expects(projectId, projectIdToPath)
      .returning(Success(maybeAccessToken))

    def givenLatestCommitIsFound(commitInfo: CommitInfo, projectId: GitLabId) =
      (latestCommitFinder
        .findLatestCommit(_: projects.GitLabId)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(commitInfo.some.pure[Try])

    def givenCommitIsInGL(commitInfo: CommitInfo, projectId: GitLabId) = (commitInfoFinder
      .getMaybeCommitInfo(_: GitLabId, _: CommitId)(_: Option[AccessToken]))
      .expects(projectId, commitInfo.id, maybeAccessToken)
      .returning(Success(commitInfo.some))

    def givenEventIsInEL(commitId: CommitId, projectId: GitLabId)(returning: CommitWithParents) =
      (eventDetailsFinder.getEventDetails _)
        .expects(projectId, commitId)
        .returning(Some(returning).pure[Try])

    def givenEventIsNotInEL(commitInfo: CommitInfo, projectId: GitLabId) =
      (eventDetailsFinder.getEventDetails _).expects(projectId, commitInfo.id).returning(Success(None))

    def givenCommitIsNotInGL(commitId: CommitId, projectId: GitLabId) =
      (commitInfoFinder
        .getMaybeCommitInfo(_: GitLabId, _: CommitId)(_: Option[AccessToken]))
        .expects(projectId, commitId, maybeAccessToken)
        .returning(Success(None))

    def givenGlobalCommitSyncRequestIsSent(project: Project) =
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          withRequestContent(project),
          EventSender.EventContext(CategoryName("GLOBAL_COMMIT_SYNC_REQUEST"),
                                   s"$categoryName: Triggering Global Commit Sync Failed"
          )
        )
        .returning(Success(()))

    def withRequestContent(project: Project) =
      EventRequestContent.NoPayload(
        json"""{"categoryName": "GLOBAL_COMMIT_SYNC_REQUEST" ,"project":{ "id": ${project.id.value}, "path":${project.path.value}}}"""
      )
  }

  private def logSummary(commitId:    CommitId,
                         project:     Project,
                         elapsedTime: ElapsedTime,
                         summary:     SynchronizationSummary
  ) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> events generation result: ${summary.show} in ${elapsedTime}ms"
  )

  private def logNewEventFound(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> new events found in ${elapsedTime}ms"
  )
  private def logEventFoundForDeletion(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> events found for deletion in ${elapsedTime}ms"
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
