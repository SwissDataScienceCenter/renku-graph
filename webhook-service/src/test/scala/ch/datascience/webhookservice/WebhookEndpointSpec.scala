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
          "before" -> JsString("95790bf891e76fee5e1747ab589903a6a1f80f22"),
          "after" -> JsString("da1560886d4f094c3e6c9ef40349f7d38b5d27d7"),
          "project_id" -> JsNumber(1)
        ).prettyPrint
      )

      Post("/webhook-event", payload) ~> routes ~> check {
        status shouldBe Accepted
        response.entity shouldBe HttpEntity.Empty
      }
    }
  }
}
