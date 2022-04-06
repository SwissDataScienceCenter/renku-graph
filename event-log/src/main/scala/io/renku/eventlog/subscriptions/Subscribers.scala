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

package io.renku.eventlog.subscriptions

import cats.effect.Async
import cats.syntax.all._
import cats.{Applicative, MonadThrow, Show}
import io.renku.events.CategoryName
import io.renku.events.consumers.subscriptions.SubscriberUrl
import org.typelevel.log4cats.Logger

private trait Subscribers[F[_], -SI <: SubscriptionInfo] {
  def add(subscriptionInfo: SI): F[Unit]

  def delete(subscriberUrl: SubscriberUrl): F[Unit]

  def markBusy(subscriberUrl: SubscriberUrl): F[Unit]

  def runOnSubscriber(f: SubscriberUrl => F[Unit]): F[Unit]

  def getTotalCapacity: Option[Capacity]
}

private class SubscribersImpl[F[_]: MonadThrow: Logger, SI <: SubscriptionInfo] private[subscriptions] (
    categoryName:        CategoryName,
    subscribersRegistry: SubscribersRegistry[F]
)(implicit
    subscriberTracker: SubscriberTracker[F, SI],
    show:              Show[SI]
) extends Subscribers[F, SI] {

  private val applicative = Applicative[F]
  import applicative._

  override def add(subscriptionInfo: SI): F[Unit] = for {
    wasAdded <- subscribersRegistry add subscriptionInfo
    _        <- subscriberTracker add subscriptionInfo
    _        <- whenA(wasAdded)(Logger[F].info(show"$categoryName: $subscriptionInfo added"))
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

  def getTotalCapacity: Option[Capacity] = subscribersRegistry.getTotalCapacity
}

private object Subscribers {

  def apply[F[_]: Async: Logger, SI <: SubscriptionInfo, ST <: SubscriberTracker[F, SI]](
      categoryName:             CategoryName
  )(implicit subscriberTracker: ST, show: Show[SI]): F[Subscribers[F, SI]] = for {
    subscribersRegistry <- SubscribersRegistry(categoryName)
    subscribers         <- MonadThrow[F].catchNonFatal(new SubscribersImpl[F, SI](categoryName, subscribersRegistry))
  } yield subscribers
}
