package io.renku.eventlog.events.categories.cleanuprequest

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class EventProcessorSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "process" should {

    "offer project id and path to the queue if a Full event is given" in new TestCase {
      val projectId   = projectIds.generateOne
      val projectPath = projectPaths.generateOne

      (eventsQueue.offer _).expects(projectId, projectPath).returning(().pure[Try])

      processor.process(CleanUpRequestEvent(projectId, projectPath)) shouldBe ().pure[Try]
    }

    "offer project id and path to the queue if a Partial event is given " +
      "but projectId was found in the project table" in new TestCase {
        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        (projectIdFinder.findProjectId _).expects(projectPath).returning(projectId.some.pure[Try])

        (eventsQueue.offer _).expects(projectId, projectPath).returning(().pure[Try])

        processor.process(CleanUpRequestEvent(projectPath)) shouldBe ().pure[Try]
      }

    "fail for a Partial event when projectId cannot be found in the project table" in new TestCase {
      val projectPath = projectPaths.generateOne

      (projectIdFinder.findProjectId _).expects(projectPath).returning(Option.empty[projects.Id].pure[Try])

      val Failure(exception) = processor.process(CleanUpRequestEvent(projectPath))

      exception.getMessage shouldBe show"Cannot find projectId for $projectPath"
    }
  }

  private trait TestCase {
    val projectIdFinder = mock[ProjectIdFinder[Try]]
    val eventsQueue     = mock[CleanUpEventsQueue[Try]]
    val processor       = new EventProcessorImpl(projectIdFinder, eventsQueue)
  }
}
