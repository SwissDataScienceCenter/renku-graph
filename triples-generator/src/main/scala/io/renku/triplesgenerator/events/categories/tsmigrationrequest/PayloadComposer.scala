package io.renku.triplesgenerator.events.categories.tsmigrationrequest

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.events.consumers.subscriptions.{SubscriberUrl, SubscriptionPayloadComposer}
import io.renku.http.server.version.ServiceVersion
import io.renku.microservices.{MicroserviceIdentifier, MicroserviceUrlFinder}

private class PayloadComposer[F[_]: MonadThrow](serviceUrlFinder: MicroserviceUrlFinder[F],
                                                serviceIdentifier: MicroserviceIdentifier,
                                                serviceVersion:    ServiceVersion
) extends SubscriptionPayloadComposer[F] {
  import io.circe.literal._
  import serviceUrlFinder._

  override def prepareSubscriptionPayload(): F[Json] = findBaseUrl()
    .map { baseUrl =>
      json"""
        {
          "categoryName": ${categoryName.value},
          "subscriber": {
            "url":     ${SubscriberUrl(baseUrl, "events").value},
            "id":      ${serviceIdentifier.value},
            "version": ${serviceVersion.value}
          }
        }"""
    }
}
