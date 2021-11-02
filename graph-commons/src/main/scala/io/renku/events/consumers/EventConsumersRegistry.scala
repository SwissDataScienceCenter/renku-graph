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

package io.renku.events.consumers

import cats.effect.IO
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult.UnsupportedEventType
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.graph.model.events.CategoryName

trait EventConsumersRegistry[F[_]] {
  def handle(requestContent: EventRequestContent): F[EventSchedulingResult]
  def renewAllSubscriptions(): F[Unit]
  def run():                   F[Unit]
}

class EventConsumersRegistryImpl[F[_]: MonadThrow: Parallel](
    eventHandlers:           List[EventHandler[F]],
    subscriptionsMechanisms: List[SubscriptionMechanism[F]]
) extends EventConsumersRegistry[F] {

  override def handle(requestContent: EventRequestContent): F[EventSchedulingResult] =
    tryNextHandler(requestContent, eventHandlers)

  private def tryNextHandler(requestContent: EventRequestContent,
                             handlers:       List[EventHandler[F]]
  ): F[EventSchedulingResult] = handlers.headOption match {
    case Some(handler) =>
      handler.tryHandling(requestContent) >>= {
        case UnsupportedEventType => tryNextHandler(requestContent, handlers.tail)
        case otherResult          => otherResult.pure[F]
      }
    case None => (UnsupportedEventType: EventSchedulingResult).pure[F]
  }

  def subscriptionMechanism(categoryName: CategoryName): F[SubscriptionMechanism[F]] =
    subscriptionsMechanisms
      .find(_.categoryName == categoryName)
      .map(_.pure[F])
      .getOrElse(
        new IllegalStateException(s"No SubscriptionMechanism for $categoryName")
          .raiseError[F, SubscriptionMechanism[F]]
      )

  def run(): F[Unit] = subscriptionsMechanisms.map(_.run()).parSequence.void

  def renewAllSubscriptions(): F[Unit] =
    subscriptionsMechanisms.map(_.renewSubscription()).parSequence.void
}

object EventConsumersRegistry {
  def apply(subscriptionFactories: (EventHandler[IO], SubscriptionMechanism[IO])*): IO[EventConsumersRegistry[IO]] =
    IO {
      new EventConsumersRegistryImpl[IO](subscriptionFactories.toList.map(_._1), subscriptionFactories.toList.map(_._2))
    }
}
