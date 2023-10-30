/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange

import cats.Applicative
import cats.syntax.all._
import io.renku.eventlog.metrics.EventStatusGauges
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus._

private[statuschange] trait GaugesUpdater[F[_]] {
  def updateGauges(dbUpdateResults: DBUpdateResults): F[Unit]
}

private[statuschange] object GaugesUpdater {
  def apply[F[_]: Applicative: EventStatusGauges]: GaugesUpdater[F] = new GaugesUpdaterImpl[F]
}

private class GaugesUpdaterImpl[F[_]: Applicative: EventStatusGauges] extends GaugesUpdater[F] {

  override def updateGauges(dbUpdateResults: DBUpdateResults): F[Unit] = dbUpdateResults match {

    case DBUpdateResults.ForProjects(projectsAndCounts) =>
      projectsAndCounts
        .map { case (slug, changedStatusCounts) =>
          def sum(of: EventStatus*): Double = changedStatusCounts.view.filterKeys(of.contains).values.sum

          List(
            EventStatusGauges[F].awaitingGeneration.update(slug -> sum(New, GenerationRecoverableFailure)),
            EventStatusGauges[F].underGeneration.update(slug    -> sum(GeneratingTriples)),
            EventStatusGauges[F].awaitingTransformation.update(
              slug -> sum(TriplesGenerated, TransformationRecoverableFailure)
            ),
            EventStatusGauges[F].underTransformation.update(slug -> sum(TransformingTriples)),
            EventStatusGauges[F].awaitingDeletion.update(slug    -> sum(AwaitingDeletion)),
            EventStatusGauges[F].underDeletion.update(slug       -> sum(Deleting))
          ).sequence
        }
        .toList
        .sequence
        .void

    case DBUpdateResults.ForAllProjects =>
      EventStatusGauges[F].asList
        .map(_.reset())
        .sequence
        .void
  }
}
