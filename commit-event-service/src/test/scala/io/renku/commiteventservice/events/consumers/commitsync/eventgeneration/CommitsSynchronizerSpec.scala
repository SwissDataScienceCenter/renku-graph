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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.commiteventservice.events.consumers.commitsync.Generators._
import io.renku.commiteventservice.events.consumers.commitsync.{categoryName, logMessageCommon}
import io.renku.commiteventservice.events.consumers.common.Generators.commitInfos
import io.renku.commiteventservice.events.consumers.common.UpdateResult._
import io.renku.commiteventservice.events.consumers.common._
import io.renku.eventlog
import io.renku.eventlog.api.events.GlobalCommitSyncRequest
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
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
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.{Clock, ZoneId, ZoneOffset}

class CommitsSynchronizerSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with BeforeAndAfterEach {

  it should "succeed if the latest eventIds in the Event Log " +
    "matches the latest commits in GitLab for relevant projects" in {

      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne.copy(event.id)

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)
      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedOnlyF(Info(show"$categoryName: $event accepted"))
    }

  it should "succeed if the latest event in the Event Log is already in EventLog and log the event as Existed" in {

    val event            = fullCommitSyncEvents.generateOne
    val latestCommitInfo = commitInfos.generateOne.copy(parents = commitIds.generateNonEmptyList().toList)

    implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)
    givenLatestCommitIsFound(latestCommitInfo, event.project.id)

    givenEventIsInEL(latestCommitInfo.id, event.project.id)(
      CommitWithParents(latestCommitInfo.id, event.project.id, parents = commitIds.generateNonEmptyList().toList)
    )

    givenCommitIsInGL(latestCommitInfo, event.project.id)

    commitsSynchronizer.synchronizeEvents(event).assertNoException >>
      logger.loggedOnlyF(
        Info(show"$categoryName: $event accepted"),
        logSummary(latestCommitInfo.id,
                   event.project,
                   executionTimeRecorder.elapsedTime,
                   SynchronizationSummary().updated(Existed, 1)
        )
      )

  }

  it should "succeed if the latest event in the Event Log has the id '0000000000000000000000000000000000000000'" +
    "log the event as skipped" in {

      val event = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne.copy(id = CommitId("0000000000000000000000000000000000000000"),
                                                          parents = List.empty[CommitId]
      )

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Skipped.name -> 1)
          )
        )
    }

  it should "succeed if there is a new commit and " +
    "the creation succeeds for the first commit and stops once ids are on both GL and EL" in {

      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id, commitIds.generateOne))

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(IO.pure(Created))

      givenEventIsInEL(parentCommit.id, event.project.id)(
        CommitWithParents(parentCommit.id, event.project.id, parentCommit.parents)
      )
      givenCommitIsInGL(parentCommit, event.project.id)
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Existed.name -> 1, Created.name -> 1)
          )
        )

    }

  it should "succeed if there is a new commit and the creation succeeds for the commit and its parents " +
    "and trigger the global commit sync process" in {

      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id))

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(IO.pure(Created))

      givenEventIsNotInEL(parentCommit, event.project.id)
      givenCommitIsInGL(parentCommit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parentCommit, batchDate)
        .returning(IO.pure(Created))
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logNewEventFound(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Created.name -> 2)
          )
        )
    }

  it should "succeed if there are no latest commit (project removed) and start the deletion process " +
    "and trigger the global commit sync process" in {

      val event        = fullCommitSyncEvents.generateOne
      val parentCommit = commitInfos.generateOne.copy(parents = List.empty[CommitId])

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      (latestCommitFinder
        .findLatestCommit(_: projects.GitLabId)(_: Option[AccessToken]))
        .expects(event.project.id, at.some)
        .returning(Option.empty[CommitInfo].pure[IO])

      givenEventIsInEL(event.id, event.project.id)(returning =
        CommitWithParents(event.id, event.project.id, List(parentCommit.id))
      )
      givenCommitIsNotInGL(event.id, event.project.id)

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, event.id)
        .returning(UpdateResult.Deleted.pure[IO])

      givenEventIsInEL(parentCommit.id, event.project.id)(returning =
        CommitWithParents(parentCommit.id, event.project.id, List.empty[CommitId])
      )
      givenCommitIsNotInGL(parentCommit.id, event.project.id)

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, parentCommit.id)
        .returning(UpdateResult.Deleted.pure[IO])
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logEventFoundForDeletion(event.id, event.project, executionTimeRecorder.elapsedTime),
          logEventFoundForDeletion(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(event.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Deleted.name -> 2)
          )
        )
    }

  it should "succeed if there is a latest commit to create and a parent to delete " +
    "and trigger the global commit sync process" in {

      val event            = fullCommitSyncEvents.generateOne
      val parentCommit     = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id))

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(IO.pure(Created))

      givenEventIsInEL(parentCommit.id, event.project.id)(returning =
        CommitWithParents(parentCommit.id, event.project.id, List.empty[CommitId])
      )
      givenCommitIsNotInGL(parentCommit.id, event.project.id)

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, parentCommit.id)
        .returning(UpdateResult.Deleted.pure[IO])
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
          logEventFoundForDeletion(parentCommit.id, event.project, executionTimeRecorder.elapsedTime),
          logSummary(latestCommitInfo.id,
                     event.project,
                     executionTimeRecorder.elapsedTime,
                     SynchronizationSummary(Deleted.name -> 1, Created.name -> 1)
          )
        )
    }

  it should "succeed and continue the process if fetching event details fails " +
    "and trigger the global commit sync process" in {

      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val parent2Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id, parent2Commit.id))

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(IO.pure(Created))

      val exception = exceptions.generateOne
      (eventDetailsFinder.getEventDetails _)
        .expects(event.project.id, parent1Commit.id)
        .returning(IO.raiseError(exception))

      givenEventIsNotInEL(parent2Commit, event.project.id)
      givenCommitIsInGL(parent2Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent2Commit, batchDate)
        .returning(IO.pure(Created))
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
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

  it should "succeed and continue the process if fetching commit info fails " +
    "and trigger the global commit sync process" in {

      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val parent2Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id, parent2Commit.id))

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(IO.pure(Created))

      givenEventIsNotInEL(parent1Commit, event.project.id)

      val exception = exceptions.generateOne
      (commitInfoFinder
        .getMaybeCommitInfo(_: GitLabId, _: CommitId)(_: Option[AccessToken]))
        .expects(event.project.id, parent1Commit.id, at.some)
        .returning(IO.raiseError(exception))

      givenEventIsNotInEL(parent2Commit, event.project.id)
      givenCommitIsInGL(parent2Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent2Commit, batchDate)
        .returning(IO.pure(Created))
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
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

  it should "succeed and continue the process if the commit creation is needed and fails " +
    "and trigger the global commit sync process" in {

      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parent1Commit.id))

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsNotInEL(latestCommitInfo, event.project.id)
      givenCommitIsInGL(latestCommitInfo, event.project.id)

      val exception = exceptions.generateOne

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, latestCommitInfo, batchDate)
        .returning(IO.pure(Failed(exception.getMessage, exception)))

      givenEventIsNotInEL(parent1Commit, event.project.id)
      givenCommitIsInGL(parent1Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent1Commit, batchDate)
        .returning(IO.pure(Created))
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
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

  it should "succeed and continue the process if the commit deletion is needed and return a failure " +
    "and trigger the global commit sync process" in {

      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsInEL(latestCommitInfo.id, event.project.id)(
        CommitWithParents(latestCommitInfo.id, event.project.id, List(parent1Commit.id))
      )
      givenCommitIsNotInGL(latestCommitInfo.id, event.project.id)

      val exception       = exceptions.generateOne
      val deletionFailure = UpdateResult.Failed(nonEmptyStrings().generateOne, exception)
      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, latestCommitInfo.id)
        .returning(deletionFailure.pure[IO])

      givenEventIsNotInEL(parent1Commit, event.project.id)
      givenCommitIsInGL(parent1Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent1Commit, batchDate)
        .returning(IO.pure(Created))
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
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

  it should "succeed and continue the process if the commit deletion is needed and fails " +
    "and trigger the global commit sync process" in {

      val event            = fullCommitSyncEvents.generateOne
      val parent1Commit    = commitInfos.generateOne.copy(parents = List.empty[CommitId])
      val latestCommitInfo = commitInfos.generateOne

      implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

      givenLatestCommitIsFound(latestCommitInfo, event.project.id)

      givenEventIsInEL(latestCommitInfo.id, event.project.id)(
        CommitWithParents(latestCommitInfo.id, event.project.id, List(parent1Commit.id))
      )
      givenCommitIsNotInGL(latestCommitInfo.id, event.project.id)

      val exception = exceptions.generateOne
      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.project, latestCommitInfo.id)
        .returning(exception.raiseError[IO, UpdateResult])

      givenEventIsNotInEL(parent1Commit, event.project.id)
      givenCommitIsInGL(parent1Commit, event.project.id)

      (commitToEventLog.storeCommitInEventLog _)
        .expects(event.project, parent1Commit, batchDate)
        .returning(IO.pure(Created))
      givenGlobalCommitSyncRequestIsSent(event.project)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
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

  it should "succeed if the process is successful but the triggering of the global commit sync fails" in {

    val event            = fullCommitSyncEvents.generateOne
    val parentCommit     = commitInfos.generateOne
    val latestCommitInfo = commitInfos.generateOne.copy(parents = List(parentCommit.id, commitIds.generateOne))

    implicit val at: AccessToken = givenAccessTokenIsFound(event.project.id)

    givenLatestCommitIsFound(latestCommitInfo, event.project.id)

    givenEventIsNotInEL(latestCommitInfo, event.project.id)
    givenCommitIsInGL(latestCommitInfo, event.project.id)

    (commitToEventLog.storeCommitInEventLog _)
      .expects(event.project, latestCommitInfo, batchDate)
      .returning(IO.pure(Created))

    givenEventIsInEL(parentCommit.id, event.project.id)(
      CommitWithParents(parentCommit.id, event.project.id, parentCommit.parents)
    )
    givenCommitIsInGL(parentCommit, event.project.id)

    val exception = exceptions.generateOne
    (elClient
      .send(_: GlobalCommitSyncRequest))
      .expects(GlobalCommitSyncRequest(event.project))
      .returning(exception.raiseError[IO, Unit])

    commitsSynchronizer.synchronizeEvents(event).assertNoException >>
      logger.loggedF(
        logNewEventFound(latestCommitInfo.id, event.project, executionTimeRecorder.elapsedTime),
        logSummary(latestCommitInfo.id,
                   event.project,
                   executionTimeRecorder.elapsedTime,
                   SynchronizationSummary(Existed.name -> 1, Created.name -> 1)
        ),
        Error(s"${logMessageCommon(event)} -> Triggering Global Commit Sync Failed", exception)
      )
  }

  it should "fail if finding Access Token for one of the event fails" in {

    val event     = fullCommitSyncEvents.generateOne
    val exception = exceptions.generateOne

    givenAccessTokenFinding(event.project.id, returning = exception.raiseError[IO, Nothing])

    commitsSynchronizer.synchronizeEvents(event).assertThrowsError[Exception](_ shouldBe exception) >>
      logger.loggedF(Error(s"${logMessageCommon(event)} -> Synchronization failed", exception))
  }

  it should "send the GlobalCommitSyncRequest if no token is found for the project" in {

    val event = commitSyncEvents.generateOne

    givenAccessTokenFinding(event.project.id, returning = Option.empty[AccessToken].pure[IO])

    givenGlobalCommitSyncRequestIsSent(event.project)

    commitsSynchronizer.synchronizeEvents(event).assertNoException >>
      logger.loggedF(Info(s"${logMessageCommon(event)} -> No access token found sending GlobalCommitSyncRequest"))
  }

  private lazy val batchDate = batchDates.generateOne

  private implicit lazy val logger:                TestLogger[IO]                = TestLogger[IO]()
  private implicit lazy val executionTimeRecorder: TestExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
  private implicit val accessTokenFinder:          AccessTokenFinder[IO]         = mock[AccessTokenFinder[IO]]
  private lazy val latestCommitFinder  = mock[LatestCommitFinder[IO]]
  private lazy val eventDetailsFinder  = mock[EventDetailsFinder[IO]]
  private lazy val commitInfoFinder    = mock[CommitInfoFinder[IO]]
  private lazy val commitToEventLog    = mock[CommitToEventLog[IO]]
  private lazy val commitEventsRemover = mock[CommitEventsRemover[IO]]
  private lazy val elClient            = mock[eventlog.api.events.Client[IO]]
  private val clock                    = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))

  private lazy val commitsSynchronizer = new CommitsSynchronizerImpl[IO](latestCommitFinder,
                                                                         eventDetailsFinder,
                                                                         commitInfoFinder,
                                                                         commitToEventLog,
                                                                         commitEventsRemover,
                                                                         elClient,
                                                                         clock
  )

  private def givenAccessTokenIsFound(projectId: GitLabId): AccessToken = {
    val at = accessTokens.generateOne
    (accessTokenFinder
      .findAccessToken(_: GitLabId)(_: GitLabId => String))
      .expects(projectId, projectIdToPath)
      .returning(at.some.pure[IO])
    at
  }

  private def givenAccessTokenFinding(projectId: GitLabId, returning: IO[Option[AccessToken]]) =
    (accessTokenFinder
      .findAccessToken(_: GitLabId)(_: GitLabId => String))
      .expects(projectId, projectIdToPath)
      .returning(returning)

  private def givenLatestCommitIsFound(commitInfo: CommitInfo, projectId: GitLabId)(implicit at: AccessToken) =
    (latestCommitFinder
      .findLatestCommit(_: projects.GitLabId)(_: Option[AccessToken]))
      .expects(projectId, at.some)
      .returning(commitInfo.some.pure[IO])

  private def givenCommitIsInGL(commitInfo: CommitInfo, projectId: GitLabId)(implicit at: AccessToken) =
    (commitInfoFinder
      .getMaybeCommitInfo(_: GitLabId, _: CommitId)(_: Option[AccessToken]))
      .expects(projectId, commitInfo.id, at.some)
      .returning(IO.pure(commitInfo.some))

  private def givenEventIsInEL(commitId: CommitId, projectId: GitLabId)(returning: CommitWithParents) =
    (eventDetailsFinder.getEventDetails _)
      .expects(projectId, commitId)
      .returning(Some(returning).pure[IO])

  private def givenEventIsNotInEL(commitInfo: CommitInfo, projectId: GitLabId) =
    (eventDetailsFinder.getEventDetails _).expects(projectId, commitInfo.id).returning(IO.pure(None))

  private def givenCommitIsNotInGL(commitId: CommitId, projectId: GitLabId)(implicit at: AccessToken) =
    (commitInfoFinder
      .getMaybeCommitInfo(_: GitLabId, _: CommitId)(_: Option[AccessToken]))
      .expects(projectId, commitId, at.some)
      .returning(IO.pure(None))

  private def givenGlobalCommitSyncRequestIsSent(project: Project) =
    (elClient
      .send(_: GlobalCommitSyncRequest))
      .expects(GlobalCommitSyncRequest(project))
      .returning(IO.unit)

  private def logSummary(commitId:    CommitId,
                         project:     Project,
                         elapsedTime: ElapsedTime,
                         summary:     SynchronizationSummary
  ) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectSlug = ${project.slug} -> events generation result: ${summary.show} in ${elapsedTime}ms"
  )

  private def logNewEventFound(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectSlug = ${project.slug} -> new events found in ${elapsedTime}ms"
  )
  private def logEventFoundForDeletion(commitId: CommitId, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectSlug = ${project.slug} -> events found for deletion in ${elapsedTime}ms"
  )

  private def logErrorSynchronization(commitId:    CommitId,
                                      project:     Project,
                                      elapsedTime: ElapsedTime,
                                      exception:   Exception,
                                      message:     String = "Synchronization failed"
  ) = Error(
    s"$categoryName: id = $commitId, projectId = ${project.id}, projectSlug = ${project.slug} -> $message in ${elapsedTime}ms",
    exception
  )

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    logger.reset()
  }
}
