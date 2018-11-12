package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.Accepted
import akka.http.scaladsl.server.{Directives, Route}
import spray.json.{JsString, JsValue, JsonReader, RootJsonReader, deserializationError}

class WebhookEndpoint(logger: LoggingAdapter) extends Directives {

  import ch.datascience.webhookservice.WebhookEndpoint.JsonSupport._

  val routes: Route =
    path("webhook-event") {
      (post & entity(as[PushEvent])) {
        pushEvent =>
          complete(HttpResponse(status = Accepted))
      }
    }
}

object WebhookEndpoint {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
  import spray.json.DefaultJsonProtocol

  def apply(logger: LoggingAdapter): WebhookEndpoint = new WebhookEndpoint(logger)

  private[webhookservice] object JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

    private implicit val projectGitUrlReads: JsonReader[RepositoryGitUrl] = {
      case JsString(value) => RepositoryGitUrl(value)
      case other           => deserializationError(s"'$other' is not a valid ProjectGitUrl")
    }
    private implicit val checkoutShaReads: JsonReader[CheckoutSha] = {
      case JsString(value) => CheckoutSha(value)
      case other           => deserializationError(s"'$other' is not a valid CheckoutSha")
    }

    implicit val pushEventFormat: RootJsonReader[PushEvent] = (json: JsValue) =>
      PushEvent(
        (json / "checkout_sha").as[CheckoutSha],
        (json / "repository" / "git_http_url").as[RepositoryGitUrl]
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