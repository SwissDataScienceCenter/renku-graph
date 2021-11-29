package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.IO
import cats.implicits.showInterpolator
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, Project}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with MockFactory with IOSpec with should.Matchers {
  "handle" should {

    "decode an event from the request, " +
      "clean up " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        (cleanupEventProcessor.process _)
          .expects(project)
          .returning(IO.unit)

        val request = requestContent(project.asJson(eventEncoder))

        handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Right(Accepted)

        logger.loggedOnly(
          Info(
            show"projectId = ${project.id}, projectPath = ${project.path}"
          )
        )
      }

    s"return $BadRequest if project path is malformed" in new TestCase {

      val request = requestContent(json"""{
        "categoryName": "CLEAN_UP",
        "project": {
          "path" :      ${projectPaths.generateOne.value}
        }
      }""")

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    val project = Project(projectIds.generateOne, projectPaths.generateOne)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val cleanupEventProcessor      = mock[EventProcessor[IO]]
    val eventBodyDeserializer      = mock[EventBodyDeserializer[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val handler = new EventHandler[IO](categoryName,
                                       cleanupEventProcessor,
                                       eventBodyDeserializer,
                                       subscriptionMechanism,
                                       concurrentProcessesLimiter
    )

    def requestContent(event: Json): EventRequestContent = EventRequestContent.NoPayload(event)
  }

  implicit lazy val eventEncoder: Encoder[Project] =
    Encoder.instance[Project] { project =>
      json"""{
        "categoryName": "CLEAN_UP",
        "project": {
        "id": ${project.id.value},
          "path" :      ${project.path.value}
        }
      }"""
    }

  private implicit class EventHandlingProcessOps(handlingProcess: IO[EventHandlingProcess[IO]]) {
    def unsafeRunSyncProcess() =
      handlingProcess.unsafeRunSync().process.value.unsafeRunSync()
  }
}
