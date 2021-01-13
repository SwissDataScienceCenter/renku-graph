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

package io.renku.eventlog.subscriptions

import cats.Semigroup
import cats.data.OptionT
import cats.effect.Effect
import ch.datascience.tinytypes.constraints.{InstantNotInTheFuture, NonBlank}
import ch.datascience.tinytypes.{InstantTinyType, StringTinyType, TinyTypeFactory}
import io.circe.Json
import io.renku.eventlog.subscriptions.SubscriptionCategory._

import java.time.Instant

private trait SubscriptionCategory[Interpretation[_]] {

  def name: CategoryName

  def run(): Interpretation[Unit]

  def register(payload: Json): Interpretation[RegistrationResult]
}

private[subscriptions] object SubscriptionCategory {

  final class CategoryName private (val value: String) extends AnyVal with StringTinyType
  implicit object CategoryName extends TinyTypeFactory[CategoryName](new CategoryName(_)) with NonBlank

  final class LastSyncedDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object LastSyncedDate
      extends TinyTypeFactory[LastSyncedDate](new LastSyncedDate(_))
      with InstantNotInTheFuture

  sealed trait RegistrationResult
  final case object AcceptedRegistration extends RegistrationResult
  final case object RejectedRegistration extends RegistrationResult

  implicit val registrationResultSemigroup: Semigroup[RegistrationResult] = {
    case (RejectedRegistration, RejectedRegistration) => RejectedRegistration
    case _                                            => AcceptedRegistration
  }
}

private class SubscriptionCategoryImpl[Interpretation[_]: Effect, PayloadType <: SubscriptionCategoryPayload](
    val name:          CategoryName,
    subscribers:       Subscribers[Interpretation],
    eventsDistributor: EventsDistributor[Interpretation],
    deserializer:      SubscriptionRequestDeserializer[Interpretation, PayloadType]
) extends SubscriptionCategory[Interpretation] {

  override def run(): Interpretation[Unit] = eventsDistributor.run()

  override def register(payload: Json): Interpretation[RegistrationResult] = (for {
    subscriptionPayload <- OptionT(deserializer.deserialize(payload))
    _                   <- OptionT.liftF(subscribers.add(subscriptionPayload.subscriberUrl))
  } yield subscriptionPayload).map(_ => AcceptedRegistration).getOrElse(RejectedRegistration)
}
