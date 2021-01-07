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

import cats._
import cats.effect.{ContextShift, Effect, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.SubscriptionCategory.{AcceptedRegistration, RejectedRegistration}
import io.renku.eventlog.subscriptions.SubscriptionCategoryRegistry.{NoCategoriesAvailable, SubscriptionResult, SuccesfulSubscription, UnsupportedPayload}

import scala.concurrent.ExecutionContext

trait SubscriptionCategoryRegistry[Interpretation[_]] {

  def run(): Interpretation[Unit]

  def register(subscriptionRequest: Json): Interpretation[SubscriptionResult]
}

private[subscriptions] object SubscriptionCategoryRegistry {
  sealed trait SubscriptionResult
  final case object SuccesfulSubscription extends SubscriptionResult
  final case class UnsupportedPayload(message: String) extends SubscriptionResult
  final case object NoCategoriesAvailable extends SubscriptionResult
}

private[subscriptions] class SubscriptionCategoryRegistryImpl[Interpretation[_]: Effect: Applicative](
    categories: Set[SubscriptionCategory[Interpretation]]
) extends SubscriptionCategoryRegistry[Interpretation] {
  override def run(): Interpretation[Unit] = categories.toList.traverse_(_.run())

  override def register(subscriptionRequest: Json): Interpretation[SubscriptionResult] =
    if (categories.isEmpty) { (NoCategoriesAvailable: SubscriptionResult).pure[Interpretation] }
    else {
      categories.toList
        .traverse(_.register(subscriptionRequest))
        .map(registrationRequests => registrationRequests.reduce(_ |+| _))
        .map {
          case AcceptedRegistration => SuccesfulSubscription
          case RejectedRegistration => UnsupportedPayload("No category supports this payload")
        }
    }
}

object IOSubscriptionCategoryRegistry {
  def apply(
      transactor:                  DbTransactor[IO, EventLogDB],
      waitingEventsGauge:          LabeledGauge[IO, projects.Path],
      underTriplesGenerationGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:            LabeledHistogram[IO, SqlQuery.Name],
      logger:                      Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[SubscriptionCategoryRegistry[IO]] =
    for {
      awaitingGenerationCategory <-
        awaitinggeneration.SubscriptionCategory(transactor,
                                                waitingEventsGauge,
                                                underTriplesGenerationGauge,
                                                queriesExecTimes,
                                                logger
        )
    } yield new SubscriptionCategoryRegistryImpl(Set[SubscriptionCategory[IO]](awaitingGenerationCategory))

}
