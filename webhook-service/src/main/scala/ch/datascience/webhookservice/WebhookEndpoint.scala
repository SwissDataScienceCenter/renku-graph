package ch.datascience.webhookservice

import akka.event.LoggingAdapter
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.{Directives, Route}

class WebhookEndpoint(logger: LoggingAdapter) extends Directives {
  val routes: Route =
    path("hello") {
      get {
        complete(ToResponseMarshallable(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>")))
      }
    }
}

object WebhookEndpoint {
  def apply(logger: LoggingAdapter): WebhookEndpoint = new WebhookEndpoint(logger)
}