package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.syntax.all._
import io.circe.Json
import io.renku.config.ServiceVersion
import io.renku.events.EventRequestContent

private trait EventDecoder {
  def decode(serviceVersion: ServiceVersion): EventRequestContent => Either[Exception, Unit]
}

private object EventDecoder extends EventDecoder {

  override def decode(serviceVersion: ServiceVersion): EventRequestContent => Either[Exception, Unit] = {
    case EventRequestContent.NoPayload(event: Json) => (decodeVersion andThenF checkMatch(serviceVersion))(event)
    case _                                          => new Exception("Invalid event").asLeft
  }

  private lazy val decodeVersion: Json => Either[Exception, ServiceVersion] =
    _.hcursor.downField("subscriber").downField("version").as[ServiceVersion]

  private def checkMatch(serviceVersion: ServiceVersion): ServiceVersion => Either[Exception, Unit] = {
    case `serviceVersion` => ().asRight
    case version          => new Exception(show"Service in version '$serviceVersion' but event for '$version'").asLeft
  }
}
