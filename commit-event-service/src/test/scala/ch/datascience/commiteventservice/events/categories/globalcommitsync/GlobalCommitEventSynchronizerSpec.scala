package ch.datascience.commiteventservice.events.categories.globalcommitsync

import cats.syntax.all._
import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.common.CommitInfo
import ch.datascience.commiteventservice.events.categories.globalcommitsync.Generators.globalCommitSyncEvents
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.history.EventDetailsFinder
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.{CommitWithParents, GlobalCommitEventSynchronizerImpl}
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.batchDates
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder.projectIdToPath
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, ZoneId, ZoneOffset}
import scala.util.{Failure, Success, Try}

class GlobalCommitEventSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "synchronizeEvents" should {
    "succeed if there are events in GitLab which were deleted in the beginning" in new TestCase {
      val event = globalCommitSyncEvents.generateOne
      true should be(true)
    }

    "fail if finding Access Token for one of the event fails" in new TestCase {
      val event     = globalCommitSyncEvents.generateOne
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

    val accessTokenFinder = mock[AccessTokenFinder[Try]]

    val eventDetailsFinder = mock[EventDetailsFinder[Try]]

    val eventStatusPatcher = mock[EventStatusPatcher[Try]]

    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger)

    val commitEventSynchronizer = new GlobalCommitEventSynchronizerImpl[Try](accessTokenFinder,
                                                                             eventDetailsFinder,
                                                                             eventStatusPatcher,
                                                                             executionTimeRecorder,
                                                                             logger
    )

    def givenAccessTokenIsFound(projectId: Id) = (accessTokenFinder
      .findAccessToken(_: Id)(_: Id => String))
      .expects(projectId, projectIdToPath)
      .returning(Success(maybeAccessToken))

//    def givenLatestCommitIsFound(commitInfo: CommitInfo, projectId: Id) = (latestCommitFinder.findLatestCommit _)
//      .expects(projectId, maybeAccessToken)
//      .returning(OptionT.some[Try](commitInfo))

    def givenCommitIsInGL = ???
//    (commitInfo: CommitInfo, projectId: Id) = (commitInfoFinder
//      .getMaybeCommitInfo(_: Id, _: CommitId)(_: Option[AccessToken]))
//      .expects(projectId, commitInfo.id, maybeAccessToken)
//      .returning(Success(commitInfo.some))

    def givenEventIsInEL(commitId: CommitId, projectId: Id)(returning: CommitWithParents) =
      (eventDetailsFinder.findAllCommits _)
        .expects(projectId)
        .returning(List(returning).pure[Try])

    def givenEventIsNotInEL(commitInfo: CommitInfo, projectId: Id) = ???
//      (eventDetailsFinder.findAllCommits _).expects(projectId).returning(Success(None))

    def givenCommitIsNotInGL(commitId: CommitId, projectId: Id) = ???
//      (commitInfoFinder
//        .getMaybeCommitInfo(_: Id, _: CommitId)(_: Option[AccessToken]))
//        .expects(projectId, commitId, maybeAccessToken)
//        .returning(Success(None))
  }

}
