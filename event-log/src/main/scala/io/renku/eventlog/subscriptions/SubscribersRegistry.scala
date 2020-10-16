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

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import cats.Applicative
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, Fiber, IO, Timer}
import cats.syntax.all._
import ch.datascience.tinytypes.{InstantTinyType, TinyTypeFactory}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

private class SubscribersRegistry(
    subscriberUrlReferenceQueue: Ref[IO, List[Deferred[IO, SubscriberUrl]]],
    now:                         () => Instant,
    logger:                      Logger[IO],
    busySleep:                   FiniteDuration,
    checkupInterval:             FiniteDuration
)(implicit contextShift:         ContextShift[IO], timer: Timer[IO], executionContext: ExecutionContext) {
  import SubscribersRegistry._

  import scala.jdk.CollectionConverters._

  private val availablePool = new ConcurrentHashMap[SubscriberUrl, Unit]()
  private val busyPool      = new ConcurrentHashMap[SubscriberUrl, CheckupTime]()

  def add(subscriberUrl: SubscriberUrl): IO[Boolean] = for {
    _        <- IO(busyPool remove subscriberUrl)
    wasAdded <- IO(Option(availablePool.putIfAbsent(subscriberUrl, ())).isEmpty)
    _        <- Applicative[IO].whenA(wasAdded)(notifyCallerAboutAvailability(subscriberUrl))
  } yield wasAdded

  private def notifyCallerAboutAvailability(subscriberUrl: SubscriberUrl): IO[Unit] = for {
    oldQueue <- shrinkCallersQueue
    _        <- oldQueue.headOption.map(notifyFirstCaller(subscriberUrl)).getOrElse(IO.unit)
  } yield ()

  private def shrinkCallersQueue = subscriberUrlReferenceQueue.getAndUpdate {
    case Nil   => Nil
    case queue => queue.tail
  }

  private def notifyFirstCaller(subscriberUrl: SubscriberUrl)(subscriberUrlReference: Deferred[IO, SubscriberUrl]) =
    for {
      _ <- subscriberUrlReference complete subscriberUrl
      _ <- notifyCallerAboutAvailability(subscriberUrl)
    } yield ()

  def findAvailableSubscriber(): IO[Deferred[IO, SubscriberUrl]] = for {
    subscriberUrlReference <- Deferred[IO, SubscriberUrl]
    _                      <- maybeSubscriberUrl map subscriberUrlReference.complete getOrElse makeCallerToWait(subscriberUrlReference)
  } yield subscriberUrlReference

  private def maybeSubscriberUrl = Random.shuffle {
    availablePool.keySet().asScala.toList
  }.headOption

  private def makeCallerToWait(subscriberUrlReference: Deferred[IO, SubscriberUrl]) = for {
    _ <- logNoFreeSubscribersInfo
    _ <- subscriberUrlReferenceQueue update (_ :+ subscriberUrlReference)
  } yield ()

  private def logNoFreeSubscribersInfo: IO[Unit] = logger.info(
    s"All ${subscriberCount()} subscribers are busy; waiting for one to become available"
  )

  def delete(subscriberUrl: SubscriberUrl): IO[Boolean] = IO {
    Option(busyPool remove subscriberUrl).isDefined |
      Option(availablePool remove subscriberUrl).isDefined
  }

  def markBusy(subscriberUrl: SubscriberUrl): IO[Unit] = IO {
    availablePool remove subscriberUrl

    val checkupTime = CheckupTime(now() plusMillis busySleep.toMillis)
    busyPool.put(subscriberUrl, checkupTime)
    ()
  }

  def subscriberCount(): Int = busyPool.size() + availablePool.size()

  private def busySubscriberCheckup(): IO[Unit] = for {
    _                        <- timer sleep checkupInterval
    subscribersDueForCheckup <- findSubscribersDueForCheckup
    _                        <- bringToAvailable(subscribersDueForCheckup)
    _                        <- busySubscriberCheckup()
  } yield ()

  private def findSubscribersDueForCheckup: IO[List[SubscriberUrl]] = IO {
    busyPool.asScala.toList
      .filter(isDueForCheckup)
      .map { case (subscriberUrl, _) => subscriberUrl }
  }

  private val isDueForCheckup: PartialFunction[(SubscriberUrl, CheckupTime), Boolean] = { case (_, checkupTime) =>
    (checkupTime.value compareTo now()) <= 0
  }

  private def bringToAvailable(subscribers: List[SubscriberUrl]): IO[List[Unit]] = subscribers.map { subscriberUrl =>
    for {
      wasAdded <- add(subscriberUrl)
      _        <- Applicative[IO].whenA(wasAdded)(logger.info(s"$subscriberUrl taken from busy state"))
    } yield ()
  }.sequence
}

private object SubscribersRegistry {

  private final class CheckupTime private (val value: Instant) extends InstantTinyType
  private object CheckupTime extends TinyTypeFactory[CheckupTime](new CheckupTime(_))

  def apply(
      logger:          Logger[IO],
      busySleep:       FiniteDuration = 5 minutes,
      checkupInterval: FiniteDuration = 500 millis
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[SubscribersRegistry] = for {
    subscriberUrlReferenceQueue <- Ref.of[IO, List[Deferred[IO, SubscriberUrl]]](List.empty)
    registry <-
      IO(new SubscribersRegistry(subscriberUrlReferenceQueue, Instant.now, logger, busySleep, checkupInterval))
    _ <- registry.busySubscriberCheckup().start
  } yield registry
}
