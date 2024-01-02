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
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, MetricsRegistry}

trait EventStatusGauges[F[_]] {
  def awaitingGeneration:     AwaitingGenerationGauge[F]
  def underGeneration:        UnderTriplesGenerationGauge[F]
  def awaitingTransformation: AwaitingTransformationGauge[F]
  def underTransformation:    UnderTransformationGauge[F]
  def awaitingDeletion:       AwaitingDeletionGauge[F]
  def underDeletion:          UnderDeletionGauge[F]
  def asList:                 List[LabeledGauge[F, projects.Slug]]
}

private class EventStatusGaugesImpl[F[_]](
    override val awaitingGeneration:     AwaitingGenerationGauge[F],
    override val underGeneration:        UnderTriplesGenerationGauge[F],
    override val awaitingTransformation: AwaitingTransformationGauge[F],
    override val underTransformation:    UnderTransformationGauge[F],
    override val awaitingDeletion:       AwaitingDeletionGauge[F],
    override val underDeletion:          UnderDeletionGauge[F]
) extends EventStatusGauges[F] {
  override def asList: List[LabeledGauge[F, projects.Slug]] = List(
    awaitingGeneration,
    underGeneration,
    awaitingTransformation,
    underTransformation,
    awaitingDeletion,
    underDeletion
  )
}

object EventStatusGauges {

  def apply[F[_]](implicit ev: EventStatusGauges[F]): EventStatusGauges[F] = ev

  def apply[F[_]: Async: MetricsRegistry](statsFinder: StatsFinder[F]): F[EventStatusGauges[F]] = for {
    awaitingGeneration     <- AwaitingGenerationGauge(statsFinder)
    underTriplesGeneration <- UnderTriplesGenerationGauge(statsFinder)
    awaitingTransformation <- AwaitingTransformationGauge(statsFinder)
    underTransformation    <- UnderTransformationGauge(statsFinder)
    awaitingDeletion <-
      AwaitingDeletionGauge(statsFinder)
    deleting <- UnderDeletionGauge(statsFinder)
  } yield new EventStatusGaugesImpl[F](awaitingGeneration,
                                       underTriplesGeneration,
                                       awaitingTransformation,
                                       underTransformation,
                                       awaitingDeletion,
                                       deleting
  )
}
