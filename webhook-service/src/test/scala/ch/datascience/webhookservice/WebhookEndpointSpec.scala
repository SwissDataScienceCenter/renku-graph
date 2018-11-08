package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class WebhookEndpointSpec extends WordSpec with ScalatestRouteTest with MockFactory {

  import WebhookEndpoint.JsonSupport._

  private val routes = new WebhookEndpoint(mock[LoggingAdapter]).routes

  "POST /webhook-event" should {

    "return ACCEPTED for valid push event payload" in {
      Post("/webhook-event", PushEvent(CommitBefore("before"), CommitAfter("after"))) ~> routes ~> check {
        status shouldBe Accepted
        response.entity shouldBe HttpEntity.Empty
      }
    }
  }
}
