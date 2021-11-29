package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.IO
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CleanUpEventProcessorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {
  "processEvent" should {
    "remove the triples linked to the project and notify the eventlog when the process is done" in new TestCase {
      (triplesRemover.removeTriples _).expects(project).returns(().pure[IO])
      (eventLogNotifier.notifyEventLog _).expects(project).returns(().pure[IO])
      cleanUpEventProcessor.process(project).unsafeRunSync() shouldBe ()
    }
    "fail if the removal of the triples fail and log the error" in new TestCase {
      val exception = exceptions.generateOne
      (triplesRemover.removeTriples _).expects(project).returns(exception.raiseError[IO, Unit])
      intercept[Exception] {
        cleanUpEventProcessor.process(project).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
      eventually {
        logger.loggedOnly(
          Error(s"${commonLogMessage(project)} - Triples removal failed ${exception.getMessage}", exception)
        )
      }
    }
    "fail if the notification of the eventlog fails and log the error" in new TestCase {
      val exception = exceptions.generateOne
      (triplesRemover.removeTriples _).expects(project).returns(().pure[IO])
      (eventLogNotifier.notifyEventLog _).expects(project).returns(exception.raiseError[IO, Unit])
      intercept[Exception] {
        cleanUpEventProcessor.process(project).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
      eventually {
        logger.loggedOnly(
          Error(
            s"${commonLogMessage(project)} - Triples removal, event log notification failed ${exception.getMessage}",
            exception
          )
        )
      }
    }
  }
  private def commonLogMessage(project: Project): String =
    s"$categoryName: ${project.show}"

  private trait TestCase {
    val project = Project(projectIds.generateOne, projectPaths.generateOne)

    implicit val logger = TestLogger[IO]()

    val triplesRemover   = mock[ProjectTriplesRemover[IO]]
    val eventLogNotifier = mock[EventLogNotifier[IO]]

    val cleanUpEventProcessor = new CleanUpEventProcessorImpl[IO](triplesRemover, eventLogNotifier)
  }
}
