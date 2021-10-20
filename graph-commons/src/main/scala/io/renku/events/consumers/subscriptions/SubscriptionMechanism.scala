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

package io.renku.events.consumers.subscriptions

import cats.data.Kleisli
import cats.effect.kernel.{Concurrent, Temporal}
import cats.effect.{Async, IO}
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import io.renku.graph.model.events.CategoryName
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

trait SubscriptionMechanism[Interpretation[_]] {
  def categoryName:        CategoryName
  def renewSubscription(): Interpretation[Unit]
  def run():               Interpretation[Unit]
}

private class SubscriptionMechanismImpl[Interpretation[_]: MonadThrow: Concurrent: Temporal: Logger](
    val categoryName:            CategoryName,
    subscriptionPayloadComposer: SubscriptionPayloadComposer[Interpretation],
    subscriptionSender:          SubscriptionSender[Interpretation],
    initialDelay:                FiniteDuration,
    renewDelay:                  FiniteDuration
) extends SubscriptionMechanism[Interpretation] {

  import cats.effect.kernel.Ref

  private val applicative = Applicative[Interpretation]

  import applicative._
  import cats.syntax.all._
  import io.circe.Json
  import subscriptionPayloadComposer._
  import subscriptionSender._

  override def renewSubscription(): Interpretation[Unit] = {
    for {
      subscriptionPayload <- prepareSubscriptionPayload()
      _                   <- postToEventLog(subscriptionPayload)
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    Logger[Interpretation].error(exception)(s"$categoryName: Problem with notifying event-log")
    exception.raiseError[Interpretation, Unit]
  }

  override def run(): Interpretation[Unit] =
    Temporal[Interpretation]
      .delayBy(Ref.of[Interpretation, Boolean](true), initialDelay)
      .flatMap(subscribeForEvents(_).foreverM[Unit])

  private def subscribeForEvents(initOrError: Ref[Interpretation, Boolean]): Interpretation[Unit] = {
    for {
      _            <- ().pure[Interpretation]
      payload      <- prepareSubscriptionPayload()
      postingError <- postToEventLog(payload).map(_ => false).recoverWith(logPostError)
      shouldLog    <- initOrError getAndSet postingError
      _            <- whenA(shouldLog && !postingError)(logInfo(payload))
      _            <- Temporal[Interpretation] sleep renewDelay
    } yield ()
  } recoverWith logSubscriberUrlError

  private lazy val logSubscriberUrlError: PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      Temporal[Interpretation].andWait(
        Logger[Interpretation].error(exception)(s"$categoryName: Composing subscription payload failed"),
        initialDelay
      )
  }

  private lazy val logPostError: PartialFunction[Throwable, Interpretation[Boolean]] = { case NonFatal(exception) =>
    Temporal[Interpretation].andWait(
      Logger[Interpretation].error(exception)(s"$categoryName: Subscribing for events failed").map(_ => true),
      initialDelay
    )
  }

  private def logInfo(payload: Json) =
    Logger[Interpretation].info(
      s"$categoryName: Subscribed for events with ${payload.subscriberUrl}, id = ${payload.subscriberId}"
    )

  private implicit class PayloadOps(json: Json) {
    lazy val subscriberId:  String = json.hcursor.downField("subscriber").downField("id").as[String].getOrElse("")
    lazy val subscriberUrl: String = json.hcursor.downField("subscriber").downField("url").as[String].getOrElse("")
  }
}

object SubscriptionMechanism {
  import com.typesafe.config.{Config, ConfigFactory}
  import io.renku.config.ConfigLoader.find

  import scala.concurrent.duration._
  import scala.language.postfixOps

  private val RenewDelay = 5 minutes

  def apply[Interpretation[_]: Async: Concurrent: Temporal: Logger](
      categoryName: CategoryName,
      subscriptionPayloadComposerFactory: Kleisli[Interpretation, CategoryName, SubscriptionPayloadComposer[
        Interpretation
      ]],
      configuration: Config = ConfigFactory.load()
  ): Interpretation[SubscriptionMechanism[Interpretation]] = for {
    initialDelay <- find[Interpretation, FiniteDuration]("event-subscription-initial-delay", configuration)
    subscriptionPayloadComposer <- subscriptionPayloadComposerFactory(categoryName)
    subscriptionSender          <- SubscriptionSender[Interpretation]
  } yield new SubscriptionMechanismImpl[Interpretation](categoryName,
                                                        subscriptionPayloadComposer,
                                                        subscriptionSender,
                                                        initialDelay,
                                                        RenewDelay
  )

  def noOpSubscriptionMechanism(category: CategoryName): SubscriptionMechanism[IO] = new SubscriptionMechanism[IO] {
    override lazy val categoryName: CategoryName = category

    override def renewSubscription(): IO[Unit] = IO.unit

    override def run(): IO[Unit] = IO.unit
  }
}
