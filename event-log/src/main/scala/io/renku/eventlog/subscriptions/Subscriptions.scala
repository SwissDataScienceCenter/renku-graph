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

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, Fiber, IO, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}

trait Subscriptions[Interpretation[_]] {
  def runOnFreeSubscriber(f: SubscriberUrl => Interpretation[Unit]): Interpretation[Unit] = ???

  def add(subscriberUrl: SubscriberUrl): Interpretation[Unit]
  def nextFree: Interpretation[Option[SubscriberUrl]]
  def isNext:   Interpretation[Boolean]
  def getAll:   Interpretation[List[SubscriberUrl]]
  def remove(subscriberUrl:   SubscriberUrl): Interpretation[Unit]
  def markBusy(subscriberUrl: SubscriberUrl): Interpretation[Unit]
}

class SubscriptionsImpl private[subscriptions] (
    currentUrl:          Ref[IO, Option[SubscriberUrl]],
    logger:              Logger[IO],
    busySleep:           FiniteDuration
)(implicit contextShift: ContextShift[IO], timer: Timer[IO], executionContext: ExecutionContext)
    extends Subscriptions[IO] {

  import scala.collection.JavaConverters._

  private val subscriptionsPool = new ConcurrentHashMap[SubscriberUrl, Unit]()
  private val onHoldPool        = new ConcurrentHashMap[SubscriberUrl, AddWithDelay]()

  override def add(subscriberUrl: SubscriberUrl): IO[Unit] =
    addAndLog(subscriberUrl, logMessage = s"$subscriberUrl added")

  private def addAndLog(subscriberUrl: SubscriberUrl, logMessage: String): IO[Unit] = IO {
    val added = Option {
      subscriptionsPool.putIfAbsent(subscriberUrl, ())
    }.isEmpty
    if (added) logger.info(logMessage)
    ()
  }

  override def nextFree: IO[Option[SubscriberUrl]] = {

    def replaceCurrentUrl(maybeUrl: Option[SubscriberUrl]) = currentUrl.set(maybeUrl) map (_ => maybeUrl)

    getAll.flatMap {
      case Nil => currentUrl.getAndSet(Option.empty[SubscriberUrl])
      case urls =>
        currentUrl.get flatMap {
          case None => currentUrl.set(urls.headOption) map (_ => urls.headOption)
          case Some(url) =>
            urls.indexOf(url) match {
              case -1                          => replaceCurrentUrl(urls.headOption)
              case idx if idx < urls.size - 1  => replaceCurrentUrl(Some(urls(idx + 1)))
              case idx if idx == urls.size - 1 => replaceCurrentUrl(urls.headOption)
            }
        }
    }
  }

  override def isNext: IO[Boolean] = (!subscriptionsPool.isEmpty).pure[IO]

  override def getAll: IO[List[SubscriberUrl]] = IO {
    subscriptionsPool.keys().asScala.toList
  }

  override def remove(subscriberUrl: SubscriberUrl): IO[Unit] =
    removeAndLog(subscriberUrl, logMessage = s"$subscriberUrl gone - removing") map (_ => ())

  private def removeAndLog(subscriberUrl: SubscriberUrl, logMessage: String): IO[Boolean] =
    for {
      removed <- IO(Option(subscriptionsPool remove subscriberUrl).isDefined)
      _       <- clearCurrentUrl(subscriberUrl)
      _       <- if (removed) logger.info(logMessage) else IO.unit
    } yield removed

  override def markBusy(subscriberUrl: SubscriberUrl): IO[Unit] =
    for {
      removed <- removeAndLog(subscriberUrl, logMessage = s"$subscriberUrl busy - putting on hold")
      _       <- clearCurrentUrl(subscriberUrl)
      _       <- if (removed) waitAndBringBack(subscriberUrl) else IO.unit
    } yield ()

  private def clearCurrentUrl(ifSetTo: SubscriberUrl): IO[Unit] =
    currentUrl.get flatMap {
      case Some(`ifSetTo`) => currentUrl.set(None)
      case _               => IO.unit
    }

  private def waitAndBringBack(subscriberUrl: SubscriberUrl): IO[Unit] =
    for {
      _ <- Option(onHoldPool.get(subscriberUrl)).fold(IO.unit)(_.cancel)
      addWithDelay = AddWithDelay(subscriberUrl)
      _ <- addWithDelay.start
      _ = onHoldPool.put(subscriberUrl, addWithDelay)
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
      currentUrl <- Ref.of[IO, Option[SubscriberUrl]](None)
    } yield new SubscriptionsImpl(currentUrl, logger, busySleep)
}
