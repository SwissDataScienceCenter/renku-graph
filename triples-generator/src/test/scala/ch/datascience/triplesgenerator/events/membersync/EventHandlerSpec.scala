package ch.datascience.triplesgenerator.events.membersync

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import ch.datascience.triplesgenerator.events.EventSchedulingResult.Accepted
import io.circe.Encoder
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.generators.Generators.Implicits._
import org.http4s.{Method, Request}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.http4s.syntax.all._
import io.circe.syntax._
import ch.datascience.http.server.EndpointTester._
import io.circe.literal._
import org.http4s.{Method, Request}

class EventHandlerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule members sync " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        (membersSynchronizer.synchronizeMembers _)
          .expects(projectPath)
          .returning(IO.unit)

        val request = Request[IO](Method.POST, uri"events").withEntity(projectPath.asJson(eventEncoder))

        handler.handle(request).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: projectPath = $projectPath -> $Accepted"
          )
        )
      }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val membersSynchronizer = mock[MembersSynchronizer[IO]]
    val logger              = TestLogger[IO]()
    val handler             = new EventHandler[IO](membersSynchronizer, logger)
  }

  implicit lazy val eventEncoder: Encoder[projects.Path] =
    Encoder.instance[projects.Path] { projectPath =>
      json"""{
        "categoryName": "MEMBER_SYNC",
        "project": {
          "path" :      ${projectPath.value}
        }
      }"""
    }
}
