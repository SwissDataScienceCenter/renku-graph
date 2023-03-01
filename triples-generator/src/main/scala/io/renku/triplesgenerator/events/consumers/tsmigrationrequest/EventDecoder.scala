package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.syntax.all._
import io.circe.{DecodingFailure, Error, Json}
import io.renku.events.EventRequestContent

private trait EventDecoder {
  val decode: EventRequestContent => Either[Exception, Unit]
}

private object EventDecoder extends EventDecoder {

  import io.renku.events.consumers.EventDecodingTools._

  override lazy val decode: EventRequestContent => Either[Exception, Unit] = {
    case EventRequestContent.NoPayload(event: Json) => (decodeVersion andThenF checkVersionSupported)(event)
    case _                                          => new Exception("Invalid event").asLeft
  }

  private lazy val decodeVersion: Json => Either[Exception, ServiceVersion] =
    _.hcursor
      .downField("subscriber")
      .downField("version")
      .as[ServiceVersion]

  private lazy val checkVersionSupported: ServiceVersion => Either[Exception, Unit] = {
    case `serviceVersion` => ().asRight
    case version          => new Exception(show"Service in version '$serviceVersion' but event for '$version'").asLeft
  }
}
