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

import cats.Applicative
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CategoryName
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

private trait Subscribers[Interpretation[_]] {
  def add(subscriptionInfo: SubscriptionInfo): Interpretation[Unit]

  def delete(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def markBusy(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def runOnSubscriber(f: SubscriberUrl => Interpretation[Unit]): Interpretation[Unit]

  def getTotalCapacity: Option[Capacity]
}

private class SubscribersImpl private[subscriptions] (
    categoryName:        CategoryName,
    subscribersRegistry: SubscribersRegistry,
    subscriberTracker:   SubscriberTracker[IO],
    logger:              Logger[IO]
)(implicit contextShift: ContextShift[IO])
    extends Subscribers[IO] {

  private val applicative = Applicative[IO]

  import applicative._

  override def add(subscriptionInfo: SubscriptionInfo): IO[Unit] = for {
    wasAdded <- subscribersRegistry add subscriptionInfo
    _        <- subscriberTracker add subscriptionInfo.subscriberUrl
    _        <- whenA(wasAdded)(logger.info(s"$categoryName: $subscriptionInfo added"))
  } yield ()

  override def delete(subscriberUrl: SubscriberUrl): IO[Unit] = for {
    removed <- subscribersRegistry delete subscriberUrl
    _       <- subscriberTracker remove subscriberUrl
    _       <- whenA(removed)(logger.info(s"$categoryName: $subscriberUrl gone - deleting"))
  } yield ()

  override def markBusy(subscriberUrl: SubscriberUrl): IO[Unit] =
    subscribersRegistry markBusy subscriberUrl

  override def runOnSubscriber(f: SubscriberUrl => IO[Unit]): IO[Unit] = for {
    subscriberUrlReference <- subscribersRegistry.findAvailableSubscriber()
    subscriberUrl          <- subscriberUrlReference.get
    _                      <- f(subscriberUrl)
  } yield ()

  def getTotalCapacity: Option[Capacity] = subscribersRegistry.getTotalCapacity
}

private object Subscribers {

  import cats.effect.IO

  def apply(
      categoryName:      CategoryName,
      subscriberTracker: SubscriberTracker[IO],
      logger:            Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[Subscribers[IO]] = for {
    subscribersRegistry <- SubscribersRegistry(categoryName, logger)
    subscribers         <- IO(new SubscribersImpl(categoryName, subscribersRegistry, subscriberTracker, logger))
  } yield subscribers
}
