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
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.tinytypes.{InstantTinyType, TinyTypeFactory}
import org.typelevel.log4cats.Logger

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.Random

private class SubscribersRegistry(
    categoryName:                CategoryName,
    subscriberUrlReferenceQueue: Ref[IO, List[Deferred[IO, SubscriberUrl]]],
    now:                         () => Instant,
    logger:                      Logger[IO],
    busySleep:                   FiniteDuration,
    checkupInterval:             FiniteDuration
)(implicit contextShift:         ContextShift[IO], timer: Timer[IO], executionContext: ExecutionContext) {

  val applicative = Applicative[IO]

  import SubscribersRegistry._
  import applicative._

  private val availablePool = new ConcurrentHashMap[SubscriptionInfo, Unit]()
  private val busyPool      = new ConcurrentHashMap[SubscriptionInfo, CheckupTime]()

  def add(subscriptionInfo: SubscriptionInfo): IO[Boolean] = for {
    _        <- IO(busyPool remove subscriptionInfo)
    exists   <- IO(Option(availablePool.get(subscriptionInfo)).nonEmpty)
    _        <- whenA(exists)(IO(availablePool.remove(subscriptionInfo)))
    wasAdded <- IO(Option(availablePool.put(subscriptionInfo, ())).isEmpty)
    _        <- whenA(wasAdded)(notifyCallerAboutAvailability(subscriptionInfo.subscriberUrl))
  } yield !exists && wasAdded

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

  private def maybeSubscriberUrl = Random
    .shuffle(availablePool.keySet().asScala.toList)
    .headOption
    .map(_.subscriberUrl)

  private def makeCallerToWait(subscriberUrlReference: Deferred[IO, SubscriberUrl]) = for {
    _ <- logNoFreeSubscribersInfo
    _ <- subscriberUrlReferenceQueue update (_ :+ subscriberUrlReference)
  } yield ()

  private def logNoFreeSubscribersInfo: IO[Unit] = logger.info(
    s"$categoryName: all ${subscriberCount()} subscriber(s) are busy; waiting for one to become available"
  )

  def delete(subscriberUrl: SubscriberUrl): IO[Boolean] = IO {
    find(subscriberUrl, in = busyPool).flatMap(info => Option(busyPool remove info)).isDefined |
      find(subscriberUrl, in = availablePool).flatMap(info => Option(availablePool remove info)).isDefined
  }

  def markBusy(subscriberUrl: SubscriberUrl): IO[Unit] = IO {
    (find(subscriberUrl, in = availablePool) orElse find(subscriberUrl, in = busyPool))
      .foreach { info =>
        availablePool.remove(info)

        val checkupTime = CheckupTime(now() plusMillis busySleep.toMillis)
        busyPool.put(info, checkupTime)
      }
  }

  def subscriberCount(): Int = busyPool.size() + availablePool.size()

  private def busySubscriberCheckup(): IO[Unit] = for {
    _                        <- timer sleep checkupInterval
    subscribersDueForCheckup <- findSubscribersDueForCheckup
    _                        <- bringToAvailable(subscribersDueForCheckup)
  } yield ()

  private def findSubscribersDueForCheckup: IO[List[SubscriptionInfo]] = IO {
    busyPool.asScala.toList
      .filter(isDueForCheckup)
      .map { case (info, _) => info }
  }

  private val isDueForCheckup: PartialFunction[(SubscriptionInfo, CheckupTime), Boolean] = { case (_, checkupTime) =>
    (checkupTime.value compareTo now()) <= 0
  }

  private def bringToAvailable(subscribers: List[SubscriptionInfo]): IO[List[Unit]] = subscribers.map { subscriberUrl =>
    for {
      wasAdded <- add(subscriberUrl)
      _        <- whenA(wasAdded)(logger.debug(s"$categoryName: $subscriberUrl taken from busy state"))
    } yield ()
  }.sequence

  private def find(subscriberUrl: SubscriberUrl, in: ConcurrentHashMap[SubscriptionInfo, _]): Option[SubscriptionInfo] =
    in.asScala
      .find { case (info, _) => info.subscriberUrl == subscriberUrl }
      .map { case (info, _) => info }

  def getTotalCapacity: Option[Capacity] =
    (availablePool.asScala.keySet ++ busyPool.asScala.keySet).toList
      .flatMap(_.maybeCapacity) match {
      case Nil        => None
      case capacities => Some(Capacity(capacities.map(_.value).sum))
    }
}

private object SubscribersRegistry {

  private final class CheckupTime private (val value: Instant) extends InstantTinyType
  private object CheckupTime extends TinyTypeFactory[CheckupTime](new CheckupTime(_))

  def apply(
      categoryName:    CategoryName,
      logger:          Logger[IO],
      busySleep:       FiniteDuration = 5 minutes,
      checkupInterval: FiniteDuration = 500 millis
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[SubscribersRegistry] = for {
    subscriberUrlReferenceQueue <- Ref.of[IO, List[Deferred[IO, SubscriberUrl]]](List.empty)
    registry <- IO {
                  new SubscribersRegistry(categoryName,
                                          subscriberUrlReferenceQueue,
                                          Instant.now _,
                                          logger,
                                          busySleep,
                                          checkupInterval
                  )
                }
    _ <- registry.busySubscriberCheckup().foreverM.start
  } yield registry
}
