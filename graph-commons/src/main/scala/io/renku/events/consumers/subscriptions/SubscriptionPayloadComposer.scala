/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.events.consumers.subscriptions

import cats.MonadThrow
import cats.data.Kleisli
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.Json
import io.renku.graph.model.events.CategoryName
import io.renku.microservices._

trait SubscriptionPayloadComposer[F[_]] {
  def prepareSubscriptionPayload(): F[Json]
}

private class SubscriptionPayloadComposerImpl[F[_]: MonadThrow](
    categoryName:           CategoryName,
    microserviceUrlFinder:  MicroserviceUrlFinder[F],
    microserviceIdentifier: MicroserviceIdentifier
) extends SubscriptionPayloadComposer[F] {

  import io.circe.syntax._
  import microserviceUrlFinder._

  override def prepareSubscriptionPayload(): F[Json] =
    findBaseUrl()
      .map(newSubscriberUrl)
      .map(SubscriberBasicInfo(_, SubscriberId(microserviceIdentifier)))
      .map(CategoryAndUrlPayload(categoryName, _).asJson)

  private def newSubscriberUrl(baseUrl: MicroserviceBaseUrl) = SubscriberUrl(baseUrl, "events")
}

object SubscriptionPayloadComposer {

  def categoryAndUrlPayloadsComposerFactory[F[_]: MonadThrow](
      microservicePort:       Int Refined Positive,
      microserviceIdentifier: MicroserviceIdentifier
  ): Kleisli[F, CategoryName, SubscriptionPayloadComposer[F]] =
    Kleisli[F, CategoryName, SubscriptionPayloadComposer[F]] { categoryName =>
      for {
        subscriptionUrlFinder <- MicroserviceUrlFinder(microservicePort)
      } yield new SubscriptionPayloadComposerImpl[F](categoryName, subscriptionUrlFinder, microserviceIdentifier)
    }
}
