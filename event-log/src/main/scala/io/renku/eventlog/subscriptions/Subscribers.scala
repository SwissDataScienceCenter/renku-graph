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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.graph.model.events.CategoryName
import org.typelevel.log4cats.Logger

private trait Subscribers[Interpretation[_]] {
  def add(subscriptionInfo: SubscriptionInfo): Interpretation[Unit]

  def delete(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def markBusy(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def runOnSubscriber(f: SubscriberUrl => Interpretation[Unit]): Interpretation[Unit]

  def getTotalCapacity: Option[Capacity]
}

private class SubscribersImpl[F[_]: MonadThrow: Logger] private[subscriptions] (
    categoryName:        CategoryName,
    subscribersRegistry: SubscribersRegistry[F],
    subscriberTracker:   SubscriberTracker[F]
) extends Subscribers[F] {

  private val moandThrow = MonadThrow[F]
  import moandThrow._

  override def add(subscriptionInfo: SubscriptionInfo): F[Unit] = for {
    wasAdded <- subscribersRegistry add subscriptionInfo
    _        <- subscriberTracker add subscriptionInfo
    _        <- whenA(wasAdded)(Logger[F].info(s"$categoryName: $subscriptionInfo added"))
  } yield ()

  override def delete(subscriberUrl: SubscriberUrl): F[Unit] = for {
    removed <- subscribersRegistry delete subscriberUrl
    _       <- subscriberTracker remove subscriberUrl
    _       <- whenA(removed)(Logger[F].info(s"$categoryName: $subscriberUrl gone - deleting"))
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

  def apply[F[_]: Async: Logger](
      categoryName:      CategoryName,
      subscriberTracker: SubscriberTracker[F]
  ): F[Subscribers[F]] = for {
    subscribersRegistry <- SubscribersRegistry(categoryName)
    subscribers <-
      MonadThrow[F].catchNonFatal(new SubscribersImpl(categoryName, subscribersRegistry, subscriberTracker))
  } yield subscribers
}
