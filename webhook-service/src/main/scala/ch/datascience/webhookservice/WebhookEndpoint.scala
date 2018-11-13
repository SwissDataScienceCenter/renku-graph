package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.QueueOfferResult
import spray.json.{JsString, JsValue, JsonReader, RootJsonReader, deserializationError}

import scala.concurrent.ExecutionContext

class WebhookEndpoint(logger: LoggingAdapter,
                      pushEventFlow: PushEventFlow)
                     (implicit executionContext: ExecutionContext) extends Directives {

  import ch.datascience.webhookservice.WebhookEndpoint.JsonSupport._

  val routes: Route =
    path("webhook-event") {
      (post & entity(as[PushEvent])) { pushEvent =>
        extractExecutionContext { implicit executionContext =>
          complete {
            pushEventFlow
              .offer(pushEvent)
              .map {
                case QueueOfferResult.Enqueued ⇒
                  logger.info(s"'$pushEvent' enqueued")
                  HttpResponse(status = Accepted)
                case other                     ⇒
                  logger.error(s"'$pushEvent' enqueueing problem: $other")
                  HttpResponse(status = InternalServerError)
              }
          }
        }
      }
    }
}

object WebhookEndpoint {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
  import spray.json.DefaultJsonProtocol

  def apply(logger: LoggingAdapter,
            pushEventFlow: PushEventFlow)
           (implicit executionContext: ExecutionContext): WebhookEndpoint = new WebhookEndpoint(logger, pushEventFlow)

  private[webhookservice] object JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

    private implicit val projectGitUrlReads: JsonReader[GitRepositoryUrl] = {
      case JsString(value) => GitRepositoryUrl(value)
      case other           => deserializationError(s"'$other' is not a valid ProjectGitUrl")
    }
    private implicit val checkoutShaReads: JsonReader[CheckoutSha] = {
      case JsString(value) => CheckoutSha(value)
      case other           => deserializationError(s"'$other' is not a valid CheckoutSha")
    }

    implicit val pushEventFormat: RootJsonReader[PushEvent] = (json: JsValue) =>
      PushEvent(
        (json / "checkout_sha").as[CheckoutSha],
        (json / "repository" / "git_http_url").as[GitRepositoryUrl]
      )

    private implicit class JsValueOps(jsValue: JsValue) {

      def /(fieldName: String): JsValue = jsValue.asJsObject.getFields(fieldName) match {
        case field +: Nil => field
        case _            => throw new IllegalArgumentException(s"'$fieldName' not found")
      }

      def as[T](implicit reads: JsonReader[T]): T = reads.read(jsValue)
    }
  }
}