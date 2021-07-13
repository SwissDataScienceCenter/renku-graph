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

package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import ch.datascience.commiteventservice.events.categories.common.UpdateResult.{Created, Deleted, Skipped}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.Generators.globalCommitSyncEventsNonZero
import ch.datascience.commiteventservice.events.categories.globalcommitsync.categoryName
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab.{GitLabCommitFetcher, GitLabCommitStatFetcher}
import ch.datascience.events.consumers.Project
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators.{batchDates, commitIds}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder.projectIdToPath
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Random, Success, Try}

class GlobalCommitEventSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "synchronizeEvents" should {
    "succeed if commits are in sync between EL and GitLab" in new TestCase {
      val event = globalCommitSyncEventsNonZero.generateOne

      givenAccessTokenIsFound(event.project.id)
      givenLatestCommitInGL(event.project.id, event.commits)

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logSummary(event.project, executionTimeRecorder.elapsedTime)
      )
    }

    "succeed if the only event in Event Log has the id '0000000000000000000000000000000000000000'" +
      "log the event as skipped" in new TestCase {
        val event              = globalCommitSyncEventsNonZero.generateOne
        val newSkippableCommit = CommitId("0000000000000000000000000000000000000000")
        val commitsInGL        = newSkippableCommit +: event.commits

        givenAccessTokenIsFound(event.project.id)
        givenLatestCommitInGL(event.project.id, commitsInGL)
        givenCommitsInGL(event.project.id, commitsInGL)
        givenNothingToDelete(event.project)

        (missingCommitEventCreator
          .createMissingCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
          .expects(event.project, List(newSkippableCommit), maybeAccessToken)
          .returning(Success(SynchronizationSummary().updated(Skipped, 1)))

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          logSummary(event.project, executionTimeRecorder.elapsedTime, skipped = 1)
        )
      }

    "fail if finding Access Token fails" in new TestCase {
      val event     = globalCommitSyncEventsNonZero.generateOne
      val exception = exceptions.generateOne

      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Failure(exception))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Failure(exception)

      logger.loggedOnly(Error(s"$categoryName - Failed to sync commits for project ${event.project}", exception))
    }

    "succeed if a commit has been deleted in GitLab but not in Event Log" +
      "and send a delete command to the event status patcher" in new TestCase {
        val event             = globalCommitSyncEventsNonZero.generateOne
        val random            = new Random()
        val commitDeletedInGL = event.commits(random.nextInt(event.commits.length))

        val commitsInGL = event.commits.filterNot(_ == commitDeletedInGL)

        givenAccessTokenIsFound(event.project.id)
        givenLatestCommitInGL(event.project.id, commitsInGL)
        givenCommitsInGL(event.project.id, commitsInGL)
        expectEventsDeletedSuccessfully(event.project, List(commitDeletedInGL))
        givenNothingToCreate(event.project)

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          logSummary(event.project, executionTimeRecorder.elapsedTime, deleted = 1)
        )

      }

    "succeed if there new commits in GitLab which don't exist in Event Log " +
      "and the creation succeeds for the commit" in new TestCase {
        val event          = globalCommitSyncEventsNonZero.generateOne
        val newCommitsInGL = commitIds.generateNonEmptyList().toList

        val commitsInGL = newCommitsInGL ++ event.commits

        givenAccessTokenIsFound(event.project.id)
        givenLatestCommitInGL(event.project.id, commitsInGL)
        givenCommitsInGL(event.project.id, commitsInGL)
        givenNothingToDelete(event.project)
        expectNewCommitsToBeCreated(event.project, newCommitsInGL)

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          logSummary(event.project, executionTimeRecorder.elapsedTime, created = newCommitsInGL.length)
        )
      }

    "succeed and delete all Event Log commits if the call for commits returns a 404" in new TestCase {
      val event = globalCommitSyncEventsNonZero.generateOne

      givenAccessTokenIsFound(event.project.id)
      givenProjectDoesntExistInGL404(event.project.id)
      expectEventsDeletedSuccessfully(event.project, event.commits)
      givenNothingToCreate(event.project)

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logSummary(event.project, executionTimeRecorder.elapsedTime, deleted = event.commits.length)
      )
    }

  }

  private trait TestCase {

    val logger = TestLogger[Try]()

    val maybeAccessToken = personalAccessTokens.generateOption
    val batchDate        = batchDates.generateOne

    val accessTokenFinder = mock[AccessTokenFinder[Try]]

    val gitLabCommitStatFetcher = mock[GitLabCommitStatFetcher[Try]]

    val gitLabCommitFetcher = mock[GitLabCommitFetcher[Try]]

    val commitEventDeleter = mock[CommitEventDeleter[Try]]

    val missingCommitEventCreator = mock[MissingCommitEventCreator[Try]]

    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger)

    val commitEventSynchronizer = new GlobalCommitEventSynchronizerImpl[Try](accessTokenFinder,
                                                                             gitLabCommitStatFetcher,
                                                                             gitLabCommitFetcher,
                                                                             commitEventDeleter,
                                                                             missingCommitEventCreator,
                                                                             executionTimeRecorder,
                                                                             logger
    )

    def givenAccessTokenIsFound(projectId: Id) = (accessTokenFinder
      .findAccessToken(_: Id)(_: Id => String))
      .expects(projectId, projectIdToPath)
      .returning(Success(maybeAccessToken))

    def givenNoCommitsInGL(projectId: Id) = {
      (gitLabCommitStatFetcher
        .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Success(ProjectCommitStats(None, 0)))

      (gitLabCommitFetcher
        .fetchGitLabCommits(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Success(List.empty[CommitId]))
    }

    def givenProjectDoesntExistInGL404(projectId: Id) =
      givenNoCommitsInGL(projectId)

    def givenLatestCommitInGL(projectId: Id, commits: List[CommitId]) =
      (gitLabCommitStatFetcher
        .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Success(ProjectCommitStats(Some(commits.head), commits.length)))

    def givenCommitsInGL(projectId: Id, commits: List[CommitId]) =
      (gitLabCommitFetcher
        .fetchGitLabCommits(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Success(commits))

    def expectEventsDeletedSuccessfully(project: Project, commits: List[CommitId]) =
      (commitEventDeleter
        .deleteExtraneousCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(project, commits, maybeAccessToken)
        .returning(Success(SynchronizationSummary().updated(Deleted, commits.length)))

    def givenNothingToDelete(project: Project) =
      (commitEventDeleter
        .deleteExtraneousCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(project, List.empty[CommitId], maybeAccessToken)
        .returning(Success(SynchronizationSummary()))

    def expectNewCommitsToBeCreated(project: Project, newCommits: List[CommitId]) =
      (missingCommitEventCreator
        .createMissingCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(project, newCommits, maybeAccessToken)
        .returning(Success(SynchronizationSummary().updated(Created, newCommits.length)))

    def givenNothingToCreate(project: Project) =
      (missingCommitEventCreator
        .createMissingCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(project, List.empty[CommitId], maybeAccessToken)
        .returning(Success(SynchronizationSummary()))
  }

  private def logSummary(project:     Project,
                         elapsedTime: ElapsedTime,
                         created:     Int = 0,
                         existed:     Int = 0,
                         skipped:     Int = 0,
                         deleted:     Int = 0,
                         failed:      Int = 0
  ) =
    Info(
      s"$categoryName: projectId = ${project.id}, projectPath = ${project.path} -> events generation result: $created created, $existed existed, $skipped skipped, $deleted deleted, $failed failed in ${elapsedTime}ms"
    )

}
