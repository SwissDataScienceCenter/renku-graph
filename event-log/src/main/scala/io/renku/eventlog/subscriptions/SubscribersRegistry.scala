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
import cats.effect._
import cats.syntax.all._
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.graph.model.events.CategoryName
import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}
import org.typelevel.log4cats.Logger

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.Random

private class SubscribersRegistry[F[_]: MonadThrow: Concurrent: Temporal: Logger](
    categoryName:                CategoryName,
    subscriberUrlReferenceQueue: Ref[F, List[Deferred[F, SubscriberUrl]]],
    now:                         () => Instant,
    busySleep:                   FiniteDuration,
    checkupInterval:             FiniteDuration
) {

  val monadThrow = MonadThrow[F]

  import SubscribersRegistry._
  import monadThrow._

  private val availablePool = new ConcurrentHashMap[SubscriptionInfo, Unit]()
  private val busyPool      = new ConcurrentHashMap[SubscriptionInfo, CheckupTime]()

  def add(subscriptionInfo: SubscriptionInfo): F[Boolean] = for {
    _        <- MonadThrow[F].catchNonFatal(busyPool remove subscriptionInfo)
    exists   <- MonadThrow[F].catchNonFatal(Option(availablePool.get(subscriptionInfo)).nonEmpty)
    _        <- whenA(exists)(MonadThrow[F].catchNonFatal(availablePool.remove(subscriptionInfo)))
    wasAdded <- MonadThrow[F].catchNonFatal(Option(availablePool.put(subscriptionInfo, ())).isEmpty)
    _        <- whenA(wasAdded)(notifyCallerAboutAvailability(subscriptionInfo.subscriberUrl))
  } yield !exists && wasAdded

  private def notifyCallerAboutAvailability(subscriberUrl: SubscriberUrl): F[Unit] = for {
    oldQueue <- shrinkCallersQueue
    _        <- oldQueue.headOption.map(notifyFirstCaller(subscriberUrl)).getOrElse(unit)
  } yield ()

  private def shrinkCallersQueue = subscriberUrlReferenceQueue.getAndUpdate {
    case Nil   => Nil
    case queue => queue.tail
  }

  private def notifyFirstCaller(subscriberUrl: SubscriberUrl)(subscriberUrlReference: Deferred[F, SubscriberUrl]) =
    for {
      _ <- subscriberUrlReference complete subscriberUrl
      _ <- notifyCallerAboutAvailability(subscriberUrl)
    } yield ()

  def findAvailableSubscriber(): F[Deferred[F, SubscriberUrl]] = for {
    subscriberUrlReference <- Deferred[F, SubscriberUrl]
    _ <- maybeSubscriberUrl map subscriberUrlReference.complete getOrElse makeCallerToWait(subscriberUrlReference)
  } yield subscriberUrlReference

  private def maybeSubscriberUrl = Random
    .shuffle(availablePool.keySet().asScala.toList)
    .headOption
    .map(_.subscriberUrl)

  private def makeCallerToWait(subscriberUrlReference: Deferred[F, SubscriberUrl]) = for {
    _ <- logNoFreeSubscribersInfo
    _ <- subscriberUrlReferenceQueue update (_ :+ subscriberUrlReference)
  } yield ()

  private def logNoFreeSubscribersInfo = Logger[F].info(
    show"$categoryName: all ${subscriberCount()} subscriber(s) are busy; waiting for one to become available"
  )

  def delete(subscriberUrl: SubscriberUrl): F[Boolean] = catchNonFatal {
    find(subscriberUrl, in = busyPool).flatMap(info => Option(busyPool remove info)).isDefined |
      find(subscriberUrl, in = availablePool).flatMap(info => Option(availablePool remove info)).isDefined
  }

  def markBusy(subscriberUrl: SubscriberUrl): F[Unit] = catchNonFatal {
    (find(subscriberUrl, in = availablePool) orElse find(subscriberUrl, in = busyPool))
      .foreach { info =>
        availablePool.remove(info)

        val checkupTime = CheckupTime(now() plusMillis busySleep.toMillis)
        busyPool.put(info, checkupTime)
      }
  }

  def subscriberCount(): Int = busyPool.size() + availablePool.size()

  private def busySubscriberCheckup(): F[Unit] = for {
    _                        <- Temporal[F] sleep checkupInterval
    subscribersDueForCheckup <- findSubscribersDueForCheckup
    _                        <- bringToAvailable(subscribersDueForCheckup)
  } yield ()

  private def findSubscribersDueForCheckup: F[List[SubscriptionInfo]] = catchNonFatal {
    busyPool.asScala.toList
      .filter(isDueForCheckup)
      .map { case (info, _) => info }
  }

  private val isDueForCheckup: PartialFunction[(SubscriptionInfo, CheckupTime), Boolean] = { case (_, checkupTime) =>
    (checkupTime.value compareTo now()) <= 0
  }

  private def bringToAvailable(subscribers: List[SubscriptionInfo]): F[List[Unit]] = subscribers.map { subscription =>
    for {
      wasAdded <- add(subscription)
      _        <- whenA(wasAdded)(Logger[F].debug(show"$categoryName: $subscription taken from busy state"))
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
  private object CheckupTime                                   extends TinyTypeFactory[CheckupTime](new CheckupTime(_))

  def apply[F[_]: MonadThrow: Spawn: Concurrent: Temporal: Logger](
      categoryName:    CategoryName,
      busySleep:       FiniteDuration = 5 minutes,
      checkupInterval: FiniteDuration = 500 millis
  ): F[SubscribersRegistry[F]] = for {
    subscriberUrlReferenceQueue <- Ref.of[F, List[Deferred[F, SubscriberUrl]]](List.empty)
    registry <- MonadThrow[F].catchNonFatal {
                  new SubscribersRegistry(categoryName,
                                          subscriberUrlReferenceQueue,
                                          Instant.now _,
                                          busySleep,
                                          checkupInterval
                  )
                }
    _ <- Spawn[F].start(registry.busySubscriberCheckup().foreverM[Unit])
  } yield registry
}
