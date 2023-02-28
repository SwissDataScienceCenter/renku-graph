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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.MonadThrow
import cats.data.Kleisli
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.events.CategoryName
import io.renku.events.Subscription.{SubscriberId, SubscriberUrl}
import io.renku.events.consumers.subscriptions.SubscriptionPayloadComposer
import io.renku.microservices.MicroserviceIdentifier

private class PayloadComposer[F[_]: MonadThrow](subscriberUrl: SubscriberUrl,
                                                serviceIdentifier: MicroserviceIdentifier,
                                                serviceVersion:    ServiceVersion
) extends SubscriptionPayloadComposer[F, MigrationsSubscription] {

  override def prepareSubscriptionPayload(): F[MigrationsSubscription] = MigrationsSubscription(
    Subscriber(subscriberUrl, SubscriberId(serviceIdentifier), serviceVersion)
  ).pure[F]
}

private object PayloadComposer {

  def payloadsComposerFactory[F[_]: MonadThrow](subscriberUrl:  SubscriberUrl,
                                                serviceId:      MicroserviceIdentifier,
                                                serviceVersion: ServiceVersion
  ): Kleisli[F, CategoryName, SubscriptionPayloadComposer[F, MigrationsSubscription]] =
    Kleisli[F, CategoryName, SubscriptionPayloadComposer[F, MigrationsSubscription]] { _ =>
      new PayloadComposer[F](subscriberUrl, serviceId, serviceVersion).pure[F].widen
    }
}
