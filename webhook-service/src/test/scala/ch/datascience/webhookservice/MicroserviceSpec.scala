package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class MicroserviceSpec extends WordSpec with ScalatestRouteTest with MockFactory {

  private val service = new WebhookEndpoint(mock[LoggingAdapter])
  import service._

  "Service" should {
    "respond to single IP query" in {
      Get("/hello") ~> routes ~> check {
        status shouldBe OK
        contentType shouldBe `text/html(UTF-8)`
        responseAs[String] shouldBe "<h1>Say hello to akka-http</h1>"
      }
    }
  }
}
