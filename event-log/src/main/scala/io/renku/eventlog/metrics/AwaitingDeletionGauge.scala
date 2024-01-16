/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.metrics

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.metrics._

trait AwaitingDeletionGauge[F[_]] extends LabeledGauge[F, projects.Slug]

object AwaitingDeletionGauge {

  val NumberOfProjects: Int Refined Positive = 20

  def apply[F[_]: Async: MetricsRegistry](statsFinder: StatsFinder[F]): F[AwaitingDeletionGauge[F]] =
    MetricsRegistry[F]
      .register {
        new PositiveValuesLabeledGauge[F, projects.Slug](
          name = "events_awaiting_deletion_count",
          help = "Number of Events waiting to clean up or re-provisioned",
          labelName = "project",
          resetDataFetch = () =>
            statsFinder
              .countEvents(Set(AwaitingDeletion), maybeLimit = Some(NumberOfProjects))
              .map(_.view.mapValues(_.toDouble).toMap)
        ) with AwaitingDeletionGauge[F]
      }
      .flatTap(_.startZeroedValuesCleaning())
      .widen
}
