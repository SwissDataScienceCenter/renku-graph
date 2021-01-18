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

package ch.datascience.triplesgenerator.events.subscriptions

import cats.Applicative
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.model.events.CategoryName
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

trait SubscriptionMechanism[Interpretation[_]] {
  def categoryName:        CategoryName
  def renewSubscription(): Interpretation[Unit]
  def run():               Interpretation[Unit]
}

class SubscriptionMechanismImpl(
    val categoryName:      CategoryName,
    subscriptionUrlFinder: SubscriptionUrlFinder[IO],
    subscriptionSender:    SubscriptionSender[IO],
    logger:                Logger[IO],
    initialDelay:          FiniteDuration,
    renewDelay:            FiniteDuration
)(implicit timer:          Timer[IO])
    extends SubscriptionMechanism[IO] {

  private val applicative = Applicative[IO]

  import applicative._
  import cats.syntax.all._
  import cats.effect.concurrent.Ref
  import subscriptionSender._
  import subscriptionUrlFinder._

  override def renewSubscription(): IO[Unit] = {
    for {
      subscriberUrl <- findSubscriberUrl()
      _             <- postToEventLog(subscriberUrl)
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    logger.error(exception)(s"$categoryName: Problem with notifying event-log")
    exception.raiseError[IO, Unit]
  }

  override def run(): IO[Unit] =
    for {
      _    <- timer sleep initialDelay
      init <- Ref.of[IO, Boolean](true)
      _    <- subscribeForEvents(init).foreverM
    } yield ()

  private def subscribeForEvents(initOrError: Ref[IO, Boolean]): IO[Unit] = {
    for {
      _             <- IO.unit
      subscriberUrl <- findSubscriberUrl()
      postingError  <- postToEventLog(subscriberUrl).map(_ => false).recoverWith(logPostError)
      shouldLog     <- initOrError getAndSet postingError
      _             <- whenA(shouldLog && !postingError)(logger.info(s"$categoryName: Subscribed for events with $subscriberUrl"))
      _             <- timer sleep renewDelay
    } yield ()
  } recoverWith logSubscriberUrlError

  private lazy val logSubscriberUrlError: PartialFunction[Throwable, IO[Unit]] = { case NonFatal(exception) =>
    for {
      _ <- logger.error(exception)(s"$categoryName: Finding subscriber URL failed")
      _ <- timer sleep initialDelay
    } yield ()
  }

  private lazy val logPostError: PartialFunction[Throwable, IO[Boolean]] = { case NonFatal(exception) =>
    for {
      _ <- logger.error(exception)(s"$categoryName: Subscribing for events failed")
      _ <- timer sleep initialDelay
    } yield true
  }
}

object SubscriptionMechanism {
  import ch.datascience.config.ConfigLoader.find
  import com.typesafe.config.{Config, ConfigFactory}

  import scala.concurrent.duration._
  import scala.language.postfixOps

  private val RenewDelay = 5 minutes

  def apply(
      categoryName:  CategoryName,
      logger:        Logger[IO],
      configuration: Config = ConfigFactory.load()
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[SubscriptionMechanism[IO]] =
    for {
      initialDelay       <- find[IO, FiniteDuration]("event-subscription-initial-delay", configuration)
      urlFinder          <- IOSubscriptionUrlFinder()
      subscriptionSender <- IOSubscriptionSender(categoryName, logger)
    } yield new SubscriptionMechanismImpl(categoryName, urlFinder, subscriptionSender, logger, initialDelay, RenewDelay)
}
