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

import cats.Applicative
import cats.effect.{ContextShift, IO, Timer}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

trait Subscribers[Interpretation[_]] {
  def add(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def delete(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def markBusy(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def runOnSubscriber(f: SubscriberUrl => Interpretation[Unit]): Interpretation[Unit]
}

class SubscribersImpl private[subscriptions] (
    subscribersRegistry: SubscribersRegistry,
    logger:              Logger[IO]
)(implicit contextShift: ContextShift[IO])
    extends Subscribers[IO] {

  override def add(subscriberUrl: SubscriberUrl): IO[Unit] = for {
    wasAdded <- subscribersRegistry add subscriberUrl
    _        <- Applicative[IO].whenA(wasAdded)(logger.info(s"$subscriberUrl added"))
  } yield ()

  override def delete(subscriberUrl: SubscriberUrl): IO[Unit] =
    for {
      removed <- subscribersRegistry delete subscriberUrl
      _       <- Applicative[IO].whenA(removed)(logger.info(s"$subscriberUrl gone - deleting"))
    } yield ()

  override def markBusy(subscriberUrl: SubscriberUrl): IO[Unit] =
    for {
      _ <- subscribersRegistry markBusy subscriberUrl
      _ <- logger.info(s"$subscriberUrl busy - putting on hold")
    } yield ()

  override def runOnSubscriber(f: SubscriberUrl => IO[Unit]): IO[Unit] =
    for {
      subscriberUrlReference <- subscribersRegistry.findAvailableSubscriber()
      subscriberUrl          <- subscriberUrlReference.get
      _                      <- f(subscriberUrl)
    } yield ()
}

object Subscribers {

  import cats.effect.IO

  def apply(
      logger: Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[Subscribers[IO]] = for {
    subscribersRegistry <- SubscribersRegistry(logger)
    subscribers         <- IO(new SubscribersImpl(subscribersRegistry, logger))
  } yield subscribers
}
