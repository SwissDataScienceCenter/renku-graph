package ch.datascience.commiteventservice.events.categories.globalcommitsync

import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.globalcommitsync.Generators.globalCommitSyncEventsNonZero
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.{GitLabCommitFetcher, GlobalCommitEventSynchronizerImpl, ProjectCommitStats}
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

import java.time.{Clock, ZoneId, ZoneOffset}
import scala.util.{Failure, Random, Success, Try}

class GlobalCommitEventSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "synchronizeEvents" should {
    "succeed if commits are in sync between EL and GitLab" in new TestCase {
      val event = globalCommitSyncEventsNonZero.generateOne

      givenAccessTokenIsFound(event.project.id)

      givenCommitsInGL(event.project.id, event.commits)

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        Info(s"${logMessageCommon(event)} -> event skipped in ${executionTimeRecorder.elapsedTime}ms")
      )
    }

    "succeed if the only event in Event Log has the id '0000000000000000000000000000000000000000'" +
      "log the event as skipped" in new TestCase {
        val event = globalCommitSyncEventsNonZero.generateOne
        fail("TBD")
      }

    "fail if finding Access Token for one of the event fails" in new TestCase {
      val event     = globalCommitSyncEventsNonZero.generateOne
      val exception = exceptions.generateOne

      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Failure(exception))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Failure(exception)

      logger.loggedOnly(Error(s"${logMessageCommon(event)} -> Synchronization failed", exception))
    }

    "succeed and send a DELETE command to Event Log if there are no events in GitLab " +
      "but there are commits in Event Log" in new TestCase {
        val event = globalCommitSyncEventsNonZero.generateOne

        givenAccessTokenIsFound(event.project.id)

        givenNoCommitsInGL(event.project.id)

        (eventStatusPatcher.sendDeletionStatus _)
          .expects(event.project.id, event.commits)
          .returning(Try(()))

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          Info(s"${logMessageCommon(event)} -> event skipped in ${executionTimeRecorder.elapsedTime}ms")
        )
      }

    "succeed if a commit has been deleted in GitLab but not in Event Log" +
      "and send a delete command to the event status patcher" in new TestCase {
        val event            = globalCommitSyncEventsNonZero.generateOne
        val random           = new Random()
        val commitIdToDelete = event.commits(random.nextInt(event.commits.length))

        val commitsInGL = event.commits.filterNot(_ == commitIdToDelete)

        (eventStatusPatcher.sendDeletionStatus _)
          .expects(event.project.id, Seq(commitIdToDelete))
          .returning(Try(()))

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())
        logger.loggedOnly(
          logEventsFoundForDeletion(CommitCount(1), event.project, executionTimeRecorder.elapsedTime),
          logSummary(CommitCount(1), event.project, executionTimeRecorder.elapsedTime, deleted = 2)
        )

      }

    "succeed if there new commits in GitLab which don't exist in Event Log " +
      "and the creation succeeds for the commit and its parents" in new TestCase {
        val event          = globalCommitSyncEventsNonZero.generateOne
        val newCommitsInGL = commitIds.generateNonEmptyList().toList

        val commitsInGL = newCommitsInGL ++ event.commits

        givenCommitsInGL(event.project.id, commitsInGL)

        (eventStatusPatcher.sendCreationStatus _)
          .expects(event.project.id, newCommitsInGL)
          .returning(Try(()))

        commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

        logger.loggedOnly(
          logEventsFoundForCreation(CommitCount(1), event.project, executionTimeRecorder.elapsedTime),
          logSummary(CommitCount(1), event.project, executionTimeRecorder.elapsedTime, deleted = 2)
        )
      }

    "succeed and delete all Event Log commits if there are no commits (project removed)" in new TestCase {
      val event = globalCommitSyncEventsNonZero.generateOne

      givenNoCommitsInGL(event.project.id)

      (eventStatusPatcher.sendDeletionStatus _)
        .expects(event.project.id, event.commits)
        .returning(Try(()))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logEventsFoundForDeletion(CommitCount(event.commits.length), event.project, executionTimeRecorder.elapsedTime),
        logSummary(CommitCount(event.commits.length), event.project, executionTimeRecorder.elapsedTime, deleted = 2)
      )
    }

    "succeed and delete all Event Log commits if the call for commits returns a 404" in new TestCase {
      val event = globalCommitSyncEventsNonZero.generateOne

      givenProjectDoesntExistInGL(event.project.id)

      (eventStatusPatcher.sendDeletionStatus _)
        .expects(event.project.id, event.commits)
        .returning(Try(()))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

      logger.loggedOnly(
        logEventsFoundForDeletion(CommitCount(event.commits.length), event.project, executionTimeRecorder.elapsedTime),
        logSummary(CommitCount(event.commits.length), event.project, executionTimeRecorder.elapsedTime, deleted = 2)
      )
    }

  }

  private trait TestCase {

    val logger = TestLogger[Try]()

    val maybeAccessToken = personalAccessTokens.generateOption
    val batchDate        = batchDates.generateOne

    val clock = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))

    val accessTokenFinder = mock[AccessTokenFinder[Try]]

    val gitLabCommitFetcher = mock[GitLabCommitFetcher[Try]]

    val eventStatusPatcher = mock[EventStatusPatcher[Try]]

    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger)

    val commitEventSynchronizer = new GlobalCommitEventSynchronizerImpl[Try](accessTokenFinder,
                                                                             gitLabCommitFetcher,
                                                                             eventStatusPatcher,
                                                                             executionTimeRecorder,
                                                                             logger
    )

    def givenAccessTokenIsFound(projectId: Id) = (accessTokenFinder
      .findAccessToken(_: Id)(_: Id => String))
      .expects(projectId, projectIdToPath)
      .returning(Success(maybeAccessToken))

    def givenNoCommitsInGL(projectId: Id) = (gitLabCommitFetcher
      .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
      .expects(projectId, maybeAccessToken)
      .returning(Success(ProjectCommitStats(None, 0)))

    def givenProjectDoesntExistInGL(projectId: Id) = (gitLabCommitFetcher
      .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
      .expects(projectId, maybeAccessToken)
      .returning(Failure(new Exception("404 Project not found")))

    def givenCommitsInGL(projectId: Id, commits: List[CommitId]) = {
      (gitLabCommitFetcher
        .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Success(ProjectCommitStats(Some(commits.head), commits.length)))

      (gitLabCommitFetcher
        .fetchAllGitLabCommits(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Success(commits))
    }

  }

  private def logEventsFoundForDeletion(commitCount: CommitCount, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: count = $commitCount, projectId = ${project.id}, projectPath = ${project.path} -> events found for deletion in ${elapsedTime}ms"
  )

  private def logEventsFoundForCreation(commitCount: CommitCount, project: Project, elapsedTime: ElapsedTime) = Info(
    s"$categoryName: count = $commitCount, projectId = ${project.id}, projectPath = ${project.path} -> events found for creation in ${elapsedTime}ms"
  )

  private def logSummary(commitCount: CommitCount,
                         project:     Project,
                         elapsedTime: ElapsedTime,
                         created:     Int = 0,
                         existed:     Int = 0,
                         skipped:     Int = 0,
                         deleted:     Int = 0,
                         failed:      Int = 0
  ) =
    Info(
      s"$categoryName: count = $commitCount, projectId = ${project.id}, projectPath = ${project.path} -> events generation result: $created created, $existed existed, $skipped skipped, $deleted deleted, $failed failed in ${elapsedTime}ms"
    )

}
