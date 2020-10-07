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
import cats.effect.{ContextShift, Fiber, IO, Timer}
import cats.syntax.all._
import ch.datascience.tinytypes.{InstantTinyType, TinyTypeFactory}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

private class SubscribersRegistry(
    busySleep:           FiniteDuration,
    now:                 () => Instant,
    logger:              Logger[IO],
    checkupInterval:     FiniteDuration = 500 millis
)(implicit contextShift: ContextShift[IO], timer: Timer[IO], executionContext: ExecutionContext) {
  import SubscribersRegistry._

  import scala.jdk.CollectionConverters._

  private val availablePool = new ConcurrentHashMap[SubscriberUrl, Unit]()
  private val busyPool      = new ConcurrentHashMap[SubscriberUrl, CheckupTime]()

  def start(notifyWhenAvailable: () => IO[Unit]): IO[Fiber[IO, Unit]] = {
    for {
      _                        <- timer sleep checkupInterval
      subscribersDueForCheckup <- findSubscribersDueForCheckup
      _                        <- bringBackAvailable(subscribersDueForCheckup, notifyWhenAvailable)
      _                        <- start(notifyWhenAvailable)
    } yield ()
  }.start

  private def bringBackAvailable(subscribers:         List[SubscriberUrl],
                                 notifyWhenAvailable: () => IO[Unit]
  ): IO[List[Unit]] = subscribers.map { subscriberUrl =>
    for {
      wasAdded <- add(subscriberUrl)
      _        <- Applicative[IO].whenA(wasAdded)(logger.info(s"$subscriberUrl taken from on hold"))
      _        <- Applicative[IO].whenA(wasAdded)(notifyWhenAvailable())
    } yield ()
  }.sequence

  def add(subscriberUrl: SubscriberUrl): IO[Boolean] = IO {
    Option {
      busyPool.remove(subscriberUrl)
      availablePool.putIfAbsent(subscriberUrl, ())
    }.isEmpty
  }

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

  def findAvailableSubscriber(): Option[SubscriberUrl] = Random.shuffle {
    availablePool.keySet().asScala.toList
  }.headOption

  def subscriberCount(): Int = busyPool.size() + availablePool.size()

  private def findSubscribersDueForCheckup: IO[List[SubscriberUrl]] = IO {
    busyPool.asScala.toList
      .filter(isDueForCheckup)
      .map { case (subscriberUrl, _) => subscriberUrl }
  }

  private val isDueForCheckup: PartialFunction[(SubscriberUrl, CheckupTime), Boolean] = { case (_, checkupTime) =>
    (checkupTime.value compareTo now()) <= 0
  }
}

private object SubscribersRegistry {

  private final class CheckupTime private (val value: Instant) extends InstantTinyType
  private object CheckupTime extends TinyTypeFactory[CheckupTime](new CheckupTime(_))

  private val busySleep: FiniteDuration = 5 minutes

  def apply(logger:     Logger[IO])(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[SubscribersRegistry] = IO {
    new SubscribersRegistry(busySleep, Instant.now, logger)
  }
}
