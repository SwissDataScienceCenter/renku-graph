package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import cats.data.OptionT
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.Generators.fullCommitSyncEvents
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.Generators.commitInfos
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.{CommitInfoFinder, EventDetailsFinder}
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import AccessTokenFinder._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult.Deleted
import ch.datascience.graph.model.projects.Id

import scala.util.{Failure, Success, Try}

class CommitEventSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "synchronizeEvents" should {

    "return unit if the latest eventIds in the Event Log " +
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

      }

    "return unit if the commit creation is needed and succeeds" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.checkIfExists _).expects(latestCommitInfo.id, event.project.id).returning(Success(false))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(latestCommitInfo.some))

      (missedEventsGenerator.generateMissedEvents _).expects(latestCommitInfo.id, event.project).returning(Success(()))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())

    }
    "return unit if the commit deletion is needed and succeeds" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.checkIfExists _).expects(latestCommitInfo.id, event.project.id).returning(Success(true))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(None))

      (commitEventsRemover.removeDeletedEvent _)
        .expects(latestCommitInfo.id, event.project)
        .returning(Success(Deleted))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())
    }
    "return unit if the commit creation and deletion are not needed" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.checkIfExists _).expects(latestCommitInfo.id, event.project.id).returning(Success(true))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(Some(commitInfos.generateOne)))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())
    }

    "return unit if there are no latest commit (project removed) and start the deletion process" in new TestCase {
      val event = fullCommitSyncEvents.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.none[Try, CommitInfo])

      (eventDetailsFinder.checkIfExists _).expects(event.id, event.project.id).returning(Success(true))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, event.id, maybeAccessToken)
        .returning(Success(None))

      (commitEventsRemover.removeDeletedEvent _)
        .expects(event.id, event.project)
        .returning(Success(Deleted))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())
    }

    "return unit if finding Access Token for one of the event fails" in new TestCase {
      val event = fullCommitSyncEvents.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Failure(exceptions.generateOne))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Success(())
    }

    "fail if if the commit creation is needed and fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.checkIfExists _).expects(latestCommitInfo.id, event.project.id).returning(Success(false))
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Success(latestCommitInfo.some))

      val exception = exceptions.generateOne
      (missedEventsGenerator.generateMissedEvents _)
        .expects(latestCommitInfo.id, event.project)
        .returning(Failure(exception))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Failure(exception)
    }

    "fail if if the commit deletion is needed and fails" in new TestCase {
      val event            = fullCommitSyncEvents.generateOne
      val latestCommitInfo = commitInfos.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Success(maybeAccessToken))
      (latestCommitFinder.findLatestCommit _)
        .expects(event.project.id, maybeAccessToken)
        .returning(OptionT.some[Try](latestCommitInfo))

      (eventDetailsFinder.checkIfExists _).expects(latestCommitInfo.id, event.project.id).returning(Success(true))

      val exception = exceptions.generateOne
      (commitInfoFinder.getMaybeCommitInfo _)
        .expects(event.project.id, latestCommitInfo.id, maybeAccessToken)
        .returning(Failure(exception))

      commitEventSynchronizer.synchronizeEvents(event) shouldBe Failure(exception)
    }
  }

  private trait TestCase {

    val maybeAccessToken   = personalAccessTokens.generateOption
    val accessTokenFinder  = mock[AccessTokenFinder[Try]]
    val latestCommitFinder = mock[LatestCommitFinder[Try]]

    val eventDetailsFinder = mock[EventDetailsFinder[Try]]
    val commitInfoFinder   = mock[CommitInfoFinder[Try]]

    val missedEventsGenerator = mock[MissedEventsGenerator[Try]]
    val commitEventsRemover   = mock[CommitEventsRemover[Try]]

    val commitEventSynchronizer = new CommitEventSynchronizerImpl[Try](accessTokenFinder,
                                                                       latestCommitFinder,
                                                                       eventDetailsFinder,
                                                                       commitInfoFinder,
                                                                       missedEventsGenerator,
                                                                       commitEventsRemover
    )
  }
}
