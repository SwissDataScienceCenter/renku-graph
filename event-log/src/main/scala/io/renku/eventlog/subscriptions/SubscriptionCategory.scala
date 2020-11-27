/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions

import cats.data.OptionT
import cats.effect.Effect
import io.circe.Json

private trait SubscriptionCategory[Interpretation[_]] {
  self =>

  protected type PayloadType <: SubscriptionCategoryPayload

  def run(): Interpretation[Unit]

  def register(payload: Json): Interpretation[Option[PayloadType]]

}

private[subscriptions] class SubscriptionCategoryImpl[Interpretation[_]: Effect, T <: SubscriptionCategoryPayload](
    subscribers:       Subscribers[Interpretation],
    eventsDistributor: EventsDistributor[Interpretation],
    deserializer:      SubscriptionRequestDeserializer[Interpretation] { type PayloadType = T }
) extends SubscriptionCategory[Interpretation] {
  override type PayloadType = T

  override def run(): Interpretation[Unit] = eventsDistributor.run()

  override def register(payload: Json): Interpretation[Option[PayloadType]] = (for {
    subscriptionPayload <- OptionT(deserializer.deserialize(payload))
    _                   <- OptionT.liftF(subscribers.add(subscriptionPayload.subscriberUrl))
  } yield subscriptionPayload).value
}
