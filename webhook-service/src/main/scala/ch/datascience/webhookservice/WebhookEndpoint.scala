package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.Accepted
import akka.http.scaladsl.server.{Directives, Route}
import spray.json.{JsString, JsValue, RootJsonFormat}

class WebhookEndpoint(logger: LoggingAdapter) extends Directives {

  import ch.datascience.webhookservice.WebhookEndpoint.JsonSupport._

  val routes: Route =
    path("webhook-event") {
      (post & entity(as[PushEvent])) {
        pushEvent =>
          println(pushEvent)
          complete(HttpResponse(status = Accepted))
      }
    }
}

object WebhookEndpoint {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
  import spray.json.DefaultJsonProtocol

  def apply(logger: LoggingAdapter): WebhookEndpoint = new WebhookEndpoint(logger)

  private[webhookservice] object JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

    private implicit val commitBeforeFormat: RootJsonFormat[CommitBefore] = new RootJsonFormat[CommitBefore] {

      import spray.json.deserializationError

      override def write(commitBefore: CommitBefore): JsValue =
        JsString(commitBefore.value)

      override def read(json: JsValue): CommitBefore = json match {
        case JsString(value) => CommitBefore(value)
        case other           => deserializationError(s"$other is not valid value for CommitBefore")
      }
    }

    private implicit val commitAfterFormat: RootJsonFormat[CommitAfter] = new RootJsonFormat[CommitAfter] {

      import spray.json.deserializationError

      override def write(commitAfter: CommitAfter): JsValue =
        JsString(commitAfter.value)

      override def read(json: JsValue): CommitAfter = json match {
        case JsString(value) => CommitAfter(value)
        case other           => deserializationError(s"$other is not valid value for CommitAfter")
      }
    }

    implicit val pushEventFormat: RootJsonFormat[PushEvent] = jsonFormat2(PushEvent.apply)
  }
}