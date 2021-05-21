package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.commitsync.Generators.fullCommitSyncEvents
import ch.datascience.commiteventservice.events.categories.commitsync.categoryName
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult.{Deleted, Failed}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CommitEventsRemoverSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "removeDeletedEvent" should {
    "return Deleted if marking the event for deletion succeeds" in new TestCase {
      val event = fullCommitSyncEvents.generateOne
      (eventPatcher.sendDeletionStatus _).expects(event.id, event.project.id).returning(Success(()))
      commitRemover.removeDeletedEvent(event.id, event.project) shouldBe Success(Deleted)
    }
    "return Ignored if marking the event for deletion fails" in new TestCase {
      val event     = fullCommitSyncEvents.generateOne
      val exception = exceptions.generateOne

      (eventPatcher.sendDeletionStatus _)
        .expects(event.id, event.project.id)
        .returning(Failure(exception))
      commitRemover.removeDeletedEvent(event.id, event.project) shouldBe Success(
        Failed(s"$categoryName - Commit Remover failed to send commit deletion status", exception)
      )
    }
  }

  private trait TestCase {
    val eventPatcher  = mock[EventStatusPatcher[Try]]
    val commitRemover = new CommitEventsRemoverImpl[Try](eventPatcher)
  }
}
