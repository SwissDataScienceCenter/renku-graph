/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.{Applicative, MonadThrow, Show}
import cats.effect.Async
import cats.syntax.all._
import io.renku.events.{CategoryName, Subscription}
import io.renku.events.Subscription.SubscriberUrl
import org.typelevel.log4cats.Logger

private trait Subscribers[F[_], -S <: Subscription.Subscriber] {
  def add(subscriber: S): F[Unit]

  def delete(subscriberUrl: SubscriberUrl): F[Unit]

  def markBusy(subscriberUrl: SubscriberUrl): F[Unit]

  def runOnSubscriber(f: SubscriberUrl => F[Unit]): F[Unit]

  def getTotalCapacity: Option[TotalCapacity]
}

private object Subscribers {

  def apply[F[_]: Async: Logger, S <: Subscription.Subscriber, ST <: SubscriberTracker[F, S]](
      categoryName: CategoryName
  )(implicit subscriberTracker: ST, show: Show[S]): F[Subscribers[F, S]] = for {
    subscribersRegistry <- SubscribersRegistry(categoryName)
    subscribers         <- MonadThrow[F].catchNonFatal(new SubscribersImpl[F, S](categoryName, subscribersRegistry))
  } yield subscribers
}

private class SubscribersImpl[F[_]: MonadThrow: Logger, S <: Subscription.Subscriber] private[producers] (
    categoryName:        CategoryName,
    subscribersRegistry: SubscribersRegistry[F]
)(implicit subscriberTracker: SubscriberTracker[F, S], show: Show[S])
    extends Subscribers[F, S] {

  private val applicative = Applicative[F]
  import applicative._

  override def add(subscriber: S): F[Unit] = for {
    wasAdded <- subscribersRegistry add subscriber
    _        <- subscriberTracker add subscriber
    _        <- whenA(wasAdded)(Logger[F].info(show"$categoryName: $subscriber added"))
  } yield ()

  override def delete(subscriberUrl: SubscriberUrl): F[Unit] = for {
    removed <- subscribersRegistry delete subscriberUrl
    _       <- subscriberTracker remove subscriberUrl
    _       <- whenA(removed)(Logger[F].info(show"$categoryName: $subscriberUrl gone - deleting"))
  } yield ()

  override def markBusy(subscriberUrl: SubscriberUrl): F[Unit] =
    subscribersRegistry markBusy subscriberUrl

  override def runOnSubscriber(f: SubscriberUrl => F[Unit]): F[Unit] = for {
    subscriberUrlReference <- subscribersRegistry.findAvailableSubscriber()
    subscriberUrl          <- subscriberUrlReference.get
    _                      <- f(subscriberUrl)
  } yield ()

  def getTotalCapacity: Option[TotalCapacity] = subscribersRegistry.getTotalCapacity
}
