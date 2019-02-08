package ch.datascience.tokenrepository

import cats.effect.IO
import ch.datascience.http.EndpointTester._
import org.http4s.dsl.io._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class PingEndpointSpec extends WordSpec {

  "ping" should {

    "respond with OK and 'pong' body" in new TestCase {
      val response = endpoint.run(
        Request(Method.GET, Uri.uri("/ping"))
      )

      response.status       shouldBe Status.Ok
      response.body[String] shouldBe "pong"
    }
  }

  private trait TestCase {
    val endpoint = new PingEndpoint[IO].ping.orNotFound
  }
}
