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

class Subscriber(
    subscriptionUrlFinder: SubscriptionUrlFinder[IO],
    subscriptionSender:    SubscriptionSender[IO],
    logger:                Logger[IO],
    initialDelay:          FiniteDuration,
    renewDelay:            FiniteDuration
)(implicit timer:          Timer[IO]) {

  import cats.implicits._
  import subscriptionSender._
  import subscriptionUrlFinder._

  def run: IO[Unit] =
    for {
      _ <- timer sleep initialDelay
      _ <- subscribeForEvents(initOrError = true)
    } yield ()

  private def subscribeForEvents(initOrError: Boolean): IO[Unit] = {
    for {
      subscriptionUrl <- findSubscriptionUrl
      _               <- send(subscriptionUrl) recoverWith errorLoggedAndRetry("Sending subscription URL failed")
      _               <- if (initOrError) logger.info(s"Subscribed for events with $subscriptionUrl") else IO.unit
      _               <- timer sleep renewDelay
      _               <- subscribeForEvents(initOrError = false)
    } yield ()
  } recoverWith errorLoggedAndRetry("Finding subscription URL failed")

  private def errorLoggedAndRetry(message: String): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)(message)
        _ <- timer sleep initialDelay
        _ <- subscribeForEvents(initOrError = true)
      } yield ()
  }
}

object Subscriber {
  import scala.concurrent.duration._
  import scala.language.postfixOps
  import ch.datascience.config.ConfigLoader.find
  import com.typesafe.config.{Config, ConfigFactory}

  private val RenewDelay = 5 minutes

  def apply(
      logger:                  Logger[IO],
      configuration:           Config = ConfigFactory.load()
  )(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]): IO[Subscriber] =
    for {
      initialDelay       <- find[IO, FiniteDuration]("event-subscription-initial-delay", configuration)
      urlFinder          <- IOSubscriptionUrlFinder()
      subscriptionSender <- IOSubscriptionSender(logger)
    } yield new Subscriber(urlFinder, subscriptionSender, logger, initialDelay, RenewDelay)
}
