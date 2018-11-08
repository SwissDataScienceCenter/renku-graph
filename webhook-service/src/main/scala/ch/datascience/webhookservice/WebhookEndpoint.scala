package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.Accepted
import akka.http.scaladsl.server.{Directives, Route}
import spray.json.{DeserializationException, JsNumber, JsString, JsValue, RootJsonReader}

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

    implicit val pushEventFormat: RootJsonReader[PushEvent] = (json: JsValue) => json.asJsObject
      .getFields("before", "after", "project_id") match {
      case Seq(JsString(before), JsString(after), JsNumber(projectId)) =>
        PushEvent(CommitBefore(before), CommitAfter(after), ProjectId(projectId.toLong))
      case _                                                           =>
        throw DeserializationException(s"Could not deserialize push event from ${json.prettyPrint}")
    }
  }
}