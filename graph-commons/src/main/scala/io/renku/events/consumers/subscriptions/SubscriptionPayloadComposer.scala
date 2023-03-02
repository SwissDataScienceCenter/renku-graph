/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.events.{CategoryName, DefaultSubscription, Subscription}
import io.renku.events.DefaultSubscription.DefaultSubscriber
import io.renku.events.Subscription._
import io.renku.microservices._

trait SubscriptionPayloadComposer[F[_], S <: Subscription] {
  def prepareSubscriptionPayload(): F[S]
}

class DefaultSubscriptionPayloadComposer[F[_]: MonadThrow](
    categoryName:           CategoryName,
    microserviceUrlFinder:  MicroserviceUrlFinder[F],
    microserviceIdentifier: MicroserviceIdentifier,
    maybeCapacity:          Option[SubscriberCapacity]
) extends SubscriptionPayloadComposer[F, DefaultSubscription] {

  import microserviceUrlFinder._

  override def prepareSubscriptionPayload(): F[DefaultSubscription] =
    buildSubscriber
      .map(DefaultSubscription(categoryName, _))

  private def findSubscriberUrl = findBaseUrl().map(SubscriberUrl(_, "events"))

  private def buildSubscriber =
    findSubscriberUrl.map { url =>
      maybeCapacity match {
        case Some(c) => DefaultSubscriber(url, SubscriberId(microserviceIdentifier), c)
        case None    => DefaultSubscriber(url, SubscriberId(microserviceIdentifier))
      }
    }
}

object SubscriptionPayloadComposer {

  def defaultSubscriptionPayloadComposerFactory[F[_]: MonadThrow](
      microservicePort:       Int Refined Positive,
      microserviceIdentifier: MicroserviceIdentifier
  ): Kleisli[F, CategoryName, SubscriptionPayloadComposer[F, DefaultSubscription]] =
    Kleisli[F, CategoryName, SubscriptionPayloadComposer[F, DefaultSubscription]] { categoryName =>
      MicroserviceUrlFinder(microservicePort).map(
        new DefaultSubscriptionPayloadComposer[F](categoryName, _, microserviceIdentifier, maybeCapacity = None)
      )
    }

  def defaultSubscriptionPayloadComposerFactory[F[_]: MonadThrow](
      microservicePort:       Int Refined Positive,
      microserviceIdentifier: MicroserviceIdentifier,
      capacity:               SubscriberCapacity
  ): Kleisli[F, CategoryName, SubscriptionPayloadComposer[F, DefaultSubscription]] =
    Kleisli[F, CategoryName, SubscriptionPayloadComposer[F, DefaultSubscription]] { categoryName =>
      MicroserviceUrlFinder(microservicePort)
        .map(
          new DefaultSubscriptionPayloadComposer[F](categoryName,
                                                    _,
                                                    microserviceIdentifier,
                                                    maybeCapacity = Some(capacity)
          )
        )
    }
}
