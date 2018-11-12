package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import spray.json._

class WebhookEndpointSpec extends WordSpec with ScalatestRouteTest with MockFactory {

  private val routes = new WebhookEndpoint(mock[LoggingAdapter]).routes

  "POST /webhook-event" should {

    "return ACCEPTED for valid push event payload" in {
      val payload = HttpEntity(
        contentType = `application/json`,
        JsObject(
          "checkout_sha" -> JsString("95790bf891e76fee5e1747ab589903a6a1f80f22"),
          "repository" -> JsObject("git_http_url" -> JsString("http://example.com/mike/repo.git"))
        ).prettyPrint
      )

      Post("/webhook-event", payload) ~> routes ~> check {
        status shouldBe Accepted
        response.entity shouldBe HttpEntity.Empty
      }
    }
  }
}
