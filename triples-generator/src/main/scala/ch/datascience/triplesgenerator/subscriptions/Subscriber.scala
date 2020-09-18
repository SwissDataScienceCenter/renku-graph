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

package ch.datascience.triplesgenerator.subscriptions

import cats.effect.{ContextShift, IO, Timer}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.control.NonFatal

trait Subscriber[Interpretation[_]] {
  def notifyAvailability: Interpretation[Unit]
  def run:                Interpretation[Unit]
}

class SubscriberImpl(
    subscriptionUrlFinder: SubscriptionUrlFinder[IO],
    subscriptionSender:    SubscriptionSender[IO],
    logger:                Logger[IO],
    initialDelay:          FiniteDuration,
    renewDelay:            FiniteDuration
)(implicit timer:          Timer[IO])
    extends Subscriber[IO] {

  import cats.syntax.all._
  import subscriptionSender._
  import subscriptionUrlFinder._

  override def notifyAvailability: IO[Unit] = {
    for {
      subscriberUrl <- findSubscriberUrl
      _             <- postToEventLog(subscriberUrl)
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    logger.error(exception)("Problem with notifying event-log")
    exception.raiseError[IO, Unit]
  }

  override def run: IO[Unit] =
    for {
      _ <- timer sleep initialDelay
      _ <- subscribeForEvents(initOrError = true)
    } yield ()

  private def subscribeForEvents(initOrError: Boolean): IO[Unit] = {
    for {
      subscriberUrl <- findSubscriberUrl
      _             <- postToEventLog(subscriberUrl) recoverWith errorLoggedAndRetry("Subscribing for events failed")
      _             <- if (initOrError) logger.info(s"Subscribed for events with $subscriberUrl") else IO.unit
      _             <- timer sleep renewDelay
      _             <- subscribeForEvents(initOrError = false)
    } yield ()
  } recoverWith errorLoggedAndRetry("Finding subscriber URL failed")

  private def errorLoggedAndRetry(message: String): PartialFunction[Throwable, IO[Unit]] = { case NonFatal(exception) =>
    for {
      _ <- logger.error(exception)(message)
      _ <- timer sleep initialDelay
      _ <- subscribeForEvents(initOrError = true)
    } yield ()
  }
}

object Subscriber {
  import ch.datascience.config.ConfigLoader.find
  import com.typesafe.config.{Config, ConfigFactory}

  import scala.concurrent.duration._
  import scala.language.postfixOps

  private val RenewDelay = 5 minutes

  def apply(
      logger:                  Logger[IO],
      configuration:           Config = ConfigFactory.load()
  )(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]): IO[Subscriber[IO]] =
    for {
      initialDelay       <- find[IO, FiniteDuration]("event-subscription-initial-delay", configuration)
      urlFinder          <- IOSubscriptionUrlFinder()
      subscriptionSender <- IOSubscriptionSender(logger)
    } yield new SubscriberImpl(urlFinder, subscriptionSender, logger, initialDelay, RenewDelay)
}
