package io.renku.eventlog.jobs

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.interpreters.TestLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.generators.CommonGraphGenerators._

import scala.concurrent.ExecutionContext.Implicits.global
import ch.datascience.generators.Generators.Implicits._
import com.github.tomakehurst.wiremock.client.WireMock.{aMultipart, aResponse, equalTo, equalToJson, post, stubFor}
import io.renku.eventlog.subscriptions.TestCategoryEvent.testCategoryEvents
import org.http4s.Status.Accepted

class K8sJobCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createJob" should {
    "call the endpoint" in new TestCase {

      stubFor {
        post("/")
          .withRequestBody(equalToJson(eventJson.spaces2))
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      jobCreator.createJob(cliVersion, event)
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)
  private trait TestCase {

    val cliVersion = cliVersions.generateOne
    val event      = testCategoryEvents.generateOne

    val jobCreator = new K8sJobCreatorImpl(TestLogger())
  }
}
