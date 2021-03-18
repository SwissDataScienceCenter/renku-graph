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
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.EventProducersRegistry.{SubscriptionResult, SuccessfulSubscription, UnsupportedPayload}
import io.renku.eventlog.subscriptions.SubscriptionCategory.{AcceptedRegistration, RejectedRegistration}

import scala.concurrent.ExecutionContext

trait EventProducersRegistry[Interpretation[_]] {
  def run(): Interpretation[Unit]
  def register(subscriptionRequest: Json): Interpretation[SubscriptionResult]
}

private[subscriptions] class EventProducersRegistryImpl[Interpretation[_]: Effect: Applicative](
    categories:      Set[SubscriptionCategory[Interpretation]]
)(implicit parallel: Parallel[Interpretation])
    extends EventProducersRegistry[Interpretation] {

  override def run(): Interpretation[Unit] = categories.toList.map(_.run()).parSequence.void

  override def register(subscriptionRequest: Json): Interpretation[SubscriptionResult] =
    if (categories.isEmpty) {
      (UnsupportedPayload("No category supports this payload"): SubscriptionResult).pure[Interpretation]
    } else {
      categories.toList
        .traverse(_ register subscriptionRequest)
        .map(registrationRequests => registrationRequests.reduce(_ |+| _))
        .map {
          case AcceptedRegistration => SuccessfulSubscription
          case RejectedRegistration => UnsupportedPayload("No category supports this payload")
        }
    }
}

object EventProducersRegistry {

  sealed trait SubscriptionResult
  final case object SuccessfulSubscription extends SubscriptionResult
  final case class UnsupportedPayload(message: String) extends SubscriptionResult

  def apply(
      transactor:                  SessionResource[IO, EventLogDB],
      waitingEventsGauge:          LabeledGauge[IO, projects.Path],
      underTriplesGenerationGauge: LabeledGauge[IO, projects.Path],
      awaitingTransformationGauge: LabeledGauge[IO, projects.Path],
      underTransformationGauge:    LabeledGauge[IO, projects.Path],
      queriesExecTimes:            LabeledHistogram[IO, SqlQuery.Name],
      microservicePort:            Int Refined Positive,
      logger:                      Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[EventProducersRegistry[IO]] = for {
    subscriberTracker <- SubscriberTracker(transactor, queriesExecTimes)
    awaitingGenerationCategory <- awaitinggeneration.SubscriptionCategory(transactor,
                                                                          waitingEventsGauge,
                                                                          underTriplesGenerationGauge,
                                                                          queriesExecTimes,
                                                                          subscriberTracker,
                                                                          logger
                                  )
    memberSyncCategory <- membersync.SubscriptionCategory(transactor, queriesExecTimes, subscriberTracker, logger)
    commitSyncCategory <- commitsync.SubscriptionCategory(transactor, queriesExecTimes, subscriberTracker, logger)
    triplesGeneratedCategory <- triplesgenerated.SubscriptionCategory(transactor,
                                                                      awaitingTransformationGauge,
                                                                      underTransformationGauge,
                                                                      queriesExecTimes,
                                                                      subscriberTracker,
                                                                      logger
                                )
    zombieEventsCategory <- zombieevents.SubscriptionCategory(transactor, queriesExecTimes, subscriberTracker, logger)
  } yield new EventProducersRegistryImpl(
    Set[SubscriptionCategory[IO]](
      awaitingGenerationCategory,
      memberSyncCategory,
      commitSyncCategory,
      triplesGeneratedCategory,
      zombieEventsCategory
    )
  )
}
