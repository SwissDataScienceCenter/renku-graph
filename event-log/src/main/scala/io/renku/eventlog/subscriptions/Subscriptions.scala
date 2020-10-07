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
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

trait Subscriptions[Interpretation[_]] {
  def add(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def delete(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def markBusy(subscriberUrl: SubscriberUrl): Interpretation[Unit]

  def runOnSubscriber(f: SubscriberUrl => Interpretation[Unit]): Interpretation[Unit]
}

private[subscriptions] trait HookReleaser[Interpretation[_]] {
  self: Subscriptions[Interpretation] =>

  private[subscriptions] val releaseHook: () => Interpretation[Unit]
}

class SubscriptionsImpl private[subscriptions] (
    executionHookContainer: Ref[IO, Option[Deferred[IO, Unit]]],
    subscribersRegistry:    SubscribersRegistry,
    logger:                 Logger[IO]
)(implicit contextShift:    ContextShift[IO])
    extends Subscriptions[IO]
    with HookReleaser[IO] {

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
    subscribersRegistry.findAvailableSubscriber() match {
      case Some(url) => f(url)
      case None =>
        for {
          executionHook <- Deferred[IO, Unit]
          _             <- executionHookContainer set executionHook.some
          _             <- logNoFreeSubscribersInfo
          _             <- executionHook.get
          _             <- runOnSubscriber(f)
        } yield ()
    }

  private def logNoFreeSubscribersInfo: IO[Unit] = logger.info(
    s"All ${subscribersRegistry.subscriberCount()} subscribers are busy; waiting for one to become available"
  )

  private[subscriptions] override val releaseHook: () => IO[Unit] = () =>
    for {
      maybeExecutionHook <- executionHookContainer getAndSet None
      _                  <- maybeExecutionHook map (_.complete(())) getOrElse IO.unit
    } yield ()
}

object Subscriptions {

  import cats.effect.IO

  def apply(logger:     Logger[IO])(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[Subscriptions[IO]] = apply(logger, None)

  private[subscriptions] def apply(logger: Logger[IO], maybeSubscribersRegistry: Option[IO[SubscribersRegistry]])(
      implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[Subscriptions[IO]] = for {
    subscribersRegistry    <- maybeSubscribersRegistry getOrElse SubscribersRegistry(logger)
    executionHookContainer <- Ref.of[IO, Option[Deferred[IO, Unit]]](None)
    subscriptions          <- IO(new SubscriptionsImpl(executionHookContainer, subscribersRegistry, logger))
    _                      <- subscribersRegistry.start(notifyWhenAvailable = subscriptions.releaseHook)
  } yield subscriptions

}
