package ch.datascience.commiteventservice.events.categories.globalcommitsync

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.globalcommitsync.Generators.globalCommitSyncEvents
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer
import ch.datascience.events.consumers.EventRequestContent
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, jsons}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventHandlerSpec     extends AnyWordSpec
  with MockFactory
  with should.Matchers
  with Eventually
  with IntegrationPatience {

  "handle" should {


    "decode an event from the request, " +
      "schedule commit synchronization " +
      s"and return $Accepted - full commit sync event case" in new TestCase {

      val event = globalCommitSyncEvents.generateOne

      (commitEventSynchronizer.synchronizeEvents _)
        .expects(event)
        .returning(().pure[IO])

      handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

      logger.loggedOnly(Info(s"${logMessageCommon(event)} -> $Accepted"))
    }

    "decode an event from the request, " +
      "schedule commit synchronization " +
      s"and return $Accepted - minimal commit sync event case" in new TestCase {

      val event = globalCommitSyncEvents.generateOne

      (commitEventSynchronizer.synchronizeEvents _)
        .expects(event)
        .returning(().pure[IO])

      handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

      logger.loggedOnly(Info(s"${logMessageCommon(event)} -> $Accepted"))
    }

    s"return $Accepted and log an error if scheduling event synchronization fails" in new TestCase {

      val event = globalCommitSyncEvents.generateOne

      (commitEventSynchronizer.synchronizeEvents _)
        .expects(event)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

      logger.getMessages(Info).map(_.message) should contain only s"${logMessageCommon(event)} -> $Accepted"

      eventually {
        logger.getMessages(Error).map(_.message) should contain only s"${logMessageCommon(event)} -> Failure"
      }
    }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      handler.handle(requestContent(jsons.generateOne.asJson)).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": ${categoryName.value}
        }"""
      }

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

  }


  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val commitEventSynchronizer = mock[GlobalCommitEventSynchronizer[IO]]
    val logger                  = TestLogger[IO]()
    val handler =
      new EventHandler[IO](categoryName, commitEventSynchronizer, logger)

    def requestContent(event: Json): EventRequestContent = EventRequestContent(event, None)
  }

  private implicit def eventEncoder[E <: GlobalCommitSyncEvent]: Encoder[E] = Encoder.instance[E] {
    case GlobalCommitSyncEvent(project, lastSynced) => json"""{
        "categoryName": "GLOBAL_COMMIT_SYNC",
        "project": {
          "id":         ${project.id.value},
          "path":       ${project.path.value}
        },
        "lastSynced":   ${lastSynced.value}
      }"""
  }

}
