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

package io.renku.eventlog.events.producers

import cats.{MonadThrow, Semigroup}
import cats.data.OptionT
import cats.syntax.all._
import io.circe.Json
import io.renku.eventlog.events.producers.SubscriptionCategory._
import io.renku.events.CategoryName

private trait `SubscriptionCategory`[F[_]] {
  val categoryName: CategoryName
  def run(): F[Unit]
  def register(payload: Json): F[RegistrationResult]
  def getStatus: F[EventProducerStatus]
}

private[producers] object SubscriptionCategory {

  sealed trait RegistrationResult
  final case object AcceptedRegistration extends RegistrationResult
  final case object RejectedRegistration extends RegistrationResult

  implicit val registrationResultSemigroup: Semigroup[RegistrationResult] = {
    case (RejectedRegistration, RejectedRegistration) => RejectedRegistration
    case _                                            => AcceptedRegistration
  }
}

private class SubscriptionCategoryImpl[F[_]: MonadThrow, SI <: SubscriptionInfo](
    override val categoryName: CategoryName,
    subscribers:               Subscribers[F, SI],
    eventsDistributor:         EventsDistributor[F],
    deserializer:              SubscriptionPayloadDeserializer[F, SI],
    capacityFinder:            CapacityFinder[F]
) extends SubscriptionCategory[F] {

  override def run(): F[Unit] = eventsDistributor.run()

  override def register(payload: Json): F[RegistrationResult] = {
    for {
      subscriptionInfo <- OptionT(deserializer deserialize payload)
      _                <- OptionT.liftF(subscribers add subscriptionInfo)
    } yield subscriptionInfo
  }.cata(default = RejectedRegistration, _ => AcceptedRegistration)

  override def getStatus: F[EventProducerStatus] =
    subscribers.getTotalCapacity match {
      case None => EventProducerStatus(categoryName, maybeCapacity = None).pure[F]
      case Some(total) =>
        capacityFinder.findUsedCapacity.map(used =>
          EventProducerStatus(categoryName, EventProducerStatus.Capacity(total, total - used).some)
        )
    }
}
