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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, Fiber, IO, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

trait Subscriptions[Interpretation[_]] {
  def add(subscriberUrl:      SubscriberUrl):                         Interpretation[Unit]
  def remove(subscriberUrl:   SubscriberUrl):                         Interpretation[Unit]
  def markBusy(subscriberUrl: SubscriberUrl):                         Interpretation[Unit]
  def runOnSubscriber(f:      SubscriberUrl => Interpretation[Unit]): Interpretation[Unit]
}

class SubscriptionsImpl private[subscriptions] (
    executionHookContainer: Ref[IO, Option[Deferred[IO, Unit]]],
    logger:                 Logger[IO],
    busySleep:              FiniteDuration
)(implicit contextShift:    ContextShift[IO], timer: Timer[IO], executionContext: ExecutionContext)
    extends Subscriptions[IO] {

  import scala.jdk.CollectionConverters._

  private val subscriptionsPool  = new ConcurrentHashMap[SubscriberUrl, Unit]()
  private val busySubscriberPool = new ConcurrentHashMap[SubscriberUrl, AddWithDelay]()

  override def add(subscriberUrl: SubscriberUrl): IO[Unit] =
    addAndLog(subscriberUrl, logMessage = s"$subscriberUrl added")

  private def addAndLog(subscriberUrl: SubscriberUrl, logMessage: String): IO[Unit] = for {
    _ <- addToThePool(subscriberUrl, logMessage)
    _ <- releaseHook()
  } yield ()

  private def addToThePool(subscriberUrl: SubscriberUrl, logMessage: String) = IO {
    val added = Option {
      subscriptionsPool.putIfAbsent(subscriberUrl, ())
    }.isEmpty
    if (added) logger.info(logMessage)
    ()
  }

  private def releaseHook() = for {
    maybeExecutionHook <- executionHookContainer.getAndSet(None)
    _ <- maybeExecutionHook match {
           case Some(executionHook) => executionHook.complete(())
           case _                   => IO.unit
         }
  } yield ()

  override def remove(subscriberUrl: SubscriberUrl): IO[Unit] =
    removeAndLog(subscriberUrl, logMessage = s"$subscriberUrl gone - removing") map (_ => ())

  override def markBusy(subscriberUrl: SubscriberUrl): IO[Unit] =
    for {
      removed <- removeAndLog(subscriberUrl, logMessage = s"$subscriberUrl busy - putting on hold")
      _       <- if (removed) waitAndBringBack(subscriberUrl) else IO.unit
    } yield ()

  private def removeAndLog(subscriberUrl: SubscriberUrl, logMessage: String): IO[Boolean] =
    for {
      removed <- IO(Option(subscriptionsPool remove subscriberUrl).isDefined)
      _       <- if (removed) logger.info(logMessage) else IO.unit
    } yield removed

  private def waitAndBringBack(subscriberUrl: SubscriberUrl): IO[Unit] =
    for {
      _ <- Option(busySubscriberPool.get(subscriberUrl)).fold(IO.unit)(_.cancel)
      addWithDelay = AddWithDelay(subscriberUrl)
      _ <- addWithDelay.start
      _ = busySubscriberPool.put(subscriberUrl, addWithDelay)
    } yield ()

  private case class AddWithDelay(subscriberUrl: SubscriberUrl) {
    private val active = new AtomicBoolean(true)

    def start: IO[Fiber[IO, Unit]] = {
      for {
        _ <- timer sleep busySleep
        _ <- if (active.get()) addAndLog(subscriberUrl, logMessage = s"$subscriberUrl taken from on hold") else IO.unit
      } yield ()
    }.start

    def cancel: IO[Unit] = IO(active.set(false))
  }

  override def runOnSubscriber(f: SubscriberUrl => IO[Unit]): IO[Unit] =
    maybeRandomSubscriber match {
      case Some(url) => f(url)
      case None =>
        for {
          executionHook <- Deferred[IO, Unit]
          _             <- executionHookContainer set executionHook.some
          _ <- logger.info(
                 s"All ${busySubscriberPool.size()} subscribers are busy; waiting for one to become available"
               )
          _ <- executionHook.get
          _ <- runOnSubscriber(f)
        } yield ()
    }

  private def maybeRandomSubscriber = Random.shuffle {
    subscriptionsPool.keySet().asScala.toList
  }.headOption
}

object Subscriptions {
  import cats.effect.IO

  private val busySleep: FiniteDuration = 5 minutes

  def apply(
      logger:    Logger[IO],
      busySleep: FiniteDuration = Subscriptions.busySleep
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[Subscriptions[IO]] =
    for {
      executionHookContainer <- Ref.of[IO, Option[Deferred[IO, Unit]]](None)
    } yield new SubscriptionsImpl(executionHookContainer, logger, busySleep)
}
