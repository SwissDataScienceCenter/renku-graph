/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.triplesgenerator.events.consumers.awaitinggeneration.subscriptions

import cats.MonadThrow
import cats.data.Kleisli
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.events.CategoryName
import io.renku.events.consumers.subscriptions.{SubscriberId, SubscriberUrl, SubscriptionPayloadComposer}
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier, MicroserviceUrlFinder}
import io.renku.triplesgenerator.Microservice
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.GenerationProcessesNumber

private[awaitinggeneration] class PayloadComposer[F[_]: MonadThrow](
    categoryName:   CategoryName,
    capacity:       GenerationProcessesNumber,
    urlFinder:      MicroserviceUrlFinder[F],
    microserviceId: MicroserviceIdentifier
) extends SubscriptionPayloadComposer[F] {
  import io.circe.syntax._
  import urlFinder._

  override def prepareSubscriptionPayload(): F[Json] =
    findBaseUrl()
      .map(newSubscriberUrl)
      .map(Subscriber(_, SubscriberId(microserviceId), capacity))
      .map(Payload(categoryName, _).asJson)

  private def newSubscriberUrl(baseUrl: MicroserviceBaseUrl) = SubscriberUrl(baseUrl, "events")
}

private[awaitinggeneration] object PayloadComposer {

  def payloadsComposerFactory[F[_]: MonadThrow]: Kleisli[F, CategoryName, SubscriptionPayloadComposer[F]] =
    Kleisli[F, CategoryName, SubscriptionPayloadComposer[F]] { categoryName =>
      for {
        subscriptionUrlFinder <- MicroserviceUrlFinder[F](Microservice.ServicePort)
        capacity              <- GenerationProcessesNumber[F]()
      } yield new PayloadComposer[F](categoryName, capacity, subscriptionUrlFinder, Microservice.Identifier)
    }
}
