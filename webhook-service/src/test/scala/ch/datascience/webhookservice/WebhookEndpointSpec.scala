package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.{Route, ValidationRejection}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.QueueOfferResult
import akka.stream.QueueOfferResult.Enqueued
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import spray.json._

import scala.concurrent.Future

class WebhookEndpointSpec extends WordSpec with ScalatestRouteTest with MockFactory {

  "POST /webhook-event" should {

    "return ACCEPTED for valid push event payload which are accepted" in new TestCase {
      val payload = HttpEntity(
        contentType = `application/json`,
        JsObject(
          "checkout_sha" -> JsString(checkoutSha.value),
          "repository" -> JsObject("git_http_url" -> JsString(repositoryUrl.value))
        ).prettyPrint
      )

      val pushEvent = PushEvent(checkoutSha, repositoryUrl)
      (pushEventQueue.offer(_: PushEvent))
        .expects(pushEvent)
        .returning(Future.successful(Enqueued))

      (logger.info(_: String))
        .expects(s"'$pushEvent' enqueued")

      Post("/webhook-event", payload) ~> routes ~> check {
        status shouldBe Accepted
        response.entity shouldBe HttpEntity.Empty
      }
    }

    QueueOfferResult.Dropped +: QueueOfferResult.QueueClosed +: QueueOfferResult.Failure(new Exception("message")) +: Nil foreach { queueOfferResult =>
      s"return INTERNAL_SERVER_ERROR for valid push event payload and queue offer result as $queueOfferResult" in new TestCase {
        val payload = HttpEntity(
          contentType = `application/json`,
          JsObject(
            "checkout_sha" -> JsString(checkoutSha.value),
            "repository" -> JsObject("git_http_url" -> JsString(repositoryUrl.value))
          ).prettyPrint
        )

        val pushEvent = PushEvent(checkoutSha, repositoryUrl)
        (pushEventQueue.offer(_: PushEvent))
          .expects(pushEvent)
          .returning(Future.successful(queueOfferResult))

        (logger.error(_: String))
          .expects(s"'$pushEvent' enqueueing problem: $queueOfferResult")

        Post("/webhook-event", payload) ~> routes ~> check {
          status shouldBe InternalServerError
          response.entity shouldBe HttpEntity.Empty
        }
      }
    }

    "return BAD_REQUEST for invalid push event payload" in new TestCase {
      val payload = HttpEntity(
        contentType = `application/json`,
        string = "{}"
      )

      Post("/webhook-event", payload) ~> routes ~> check {
        handled shouldBe false
        rejection shouldBe a[ValidationRejection]
      }
    }
  }


  private trait TestCase {
    val checkoutSha: CheckoutSha = ServiceTypesGenerators.checkoutShas.generateOne
    val repositoryUrl = GitRepositoryUrl("http://example.com/mike/repo.git")

    val pushEventQueue: PushEventQueue = mock[PushEventQueue]
    val logger: LoggingAdapter = mock[LoggingAdapter]
    val routes: Route = new WebhookEndpoint(logger, pushEventQueue).routes
  }
}
