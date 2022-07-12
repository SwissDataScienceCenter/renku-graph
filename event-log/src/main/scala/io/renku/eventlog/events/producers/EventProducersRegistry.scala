/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers

import cats._
import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers.EventProducersRegistry.{SubscriptionResult, SuccessfulSubscription, UnsupportedPayload}
import io.renku.eventlog.events.producers.SubscriptionCategory.{AcceptedRegistration, RejectedRegistration}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram, MetricsRegistry}
import org.typelevel.log4cats.Logger

trait EventProducersRegistry[F[_]] {
  def run(): F[Unit]
  def register(subscriptionRequest: Json): F[SubscriptionResult]
}

private[producers] class EventProducersRegistryImpl[F[_]: Parallel: Applicative](
    categories: Set[SubscriptionCategory[F]]
) extends EventProducersRegistry[F] {

  override def run(): F[Unit] = categories.toList.map(_.run()).parSequence.void

  override def register(subscriptionRequest: Json): F[SubscriptionResult] =
    if (categories.isEmpty) {
      (UnsupportedPayload("No category supports this payload"): SubscriptionResult).pure[F]
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
  final case object SuccessfulSubscription             extends SubscriptionResult
  final case class UnsupportedPayload(message: String) extends SubscriptionResult

  def apply[F[_]: Async: Parallel: SessionResource: Logger: MetricsRegistry](
      awaitingGenerationGauge:     LabeledGauge[F, projects.Path],
      underGenerationGauge:        LabeledGauge[F, projects.Path],
      awaitingTransformationGauge: LabeledGauge[F, projects.Path],
      underTransformationGauge:    LabeledGauge[F, projects.Path],
      awaitingDeletionGauge:       LabeledGauge[F, projects.Path],
      deletingGauge:               LabeledGauge[F, projects.Path],
      queriesExecTimes:            LabeledHistogram[F]
  ): F[EventProducersRegistry[F]] = UrlAndIdSubscriberTracker(queriesExecTimes).flatMap { implicit subscriberTracker =>
    for {
      awaitingGenerationCategory <-
        awaitinggeneration.SubscriptionCategory(awaitingGenerationGauge, underGenerationGauge, queriesExecTimes)
      memberSyncCategory       <- membersync.SubscriptionCategory(queriesExecTimes)
      commitSyncCategory       <- commitsync.SubscriptionCategory(queriesExecTimes)
      globalCommitSyncCategory <- globalcommitsync.SubscriptionCategory(queriesExecTimes)
      projectSyncCategory      <- projectsync.SubscriptionCategory(queriesExecTimes)
      triplesGeneratedCategory <-
        triplesgenerated.SubscriptionCategory(awaitingTransformationGauge, underTransformationGauge, queriesExecTimes)
      cleanUpEventCategory   <- cleanup.SubscriptionCategory(awaitingDeletionGauge, deletingGauge, queriesExecTimes)
      zombieEventsCategory   <- zombieevents.SubscriptionCategory(queriesExecTimes)
      tsMigrationCategory    <- tsmigrationrequest.SubscriptionCategory(queriesExecTimes)
      minProjectInfoCategory <- minprojectinfo.SubscriptionCategory(queriesExecTimes)
    } yield new EventProducersRegistryImpl(
      Set(
        awaitingGenerationCategory,
        memberSyncCategory,
        commitSyncCategory,
        globalCommitSyncCategory,
        projectSyncCategory,
        triplesGeneratedCategory,
        cleanUpEventCategory,
        zombieEventsCategory,
        tsMigrationCategory,
        minProjectInfoCategory
      )
    )
  }
}