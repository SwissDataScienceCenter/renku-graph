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

package io.renku.eventlog.events.categories.statuschange

import cats.Applicative
import cats.syntax.all._
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge

private trait GaugesUpdater[Interpretation[_]] {
  def updateGauges(dbUpdateResults: DBUpdateResults): Interpretation[Unit]
}

private class GaugesUpdaterImpl[Interpretation[_]: Applicative](
    awaitingGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    underTransformationGauge:    LabeledGauge[Interpretation, projects.Path],
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path]
) extends GaugesUpdater[Interpretation] {

  override def updateGauges(dbUpdateResults: DBUpdateResults): Interpretation[Unit] = dbUpdateResults match {

    case DBUpdateResults.ForProjects(projectsAndCounts) =>
      projectsAndCounts
        .map { case (projectPath, changedStatusCounts) =>
          def sum(of: EventStatus*): Double =
            changedStatusCounts.view.filterKeys(of.contains).values.sum
          List(
            awaitingGenerationGauge.update(projectPath     -> sum(New, GenerationRecoverableFailure)),
            underTriplesGenerationGauge.update(projectPath -> sum(GeneratingTriples)),
            awaitingTransformationGauge.update(projectPath -> sum(TriplesGenerated, TransformationRecoverableFailure)),
            underTransformationGauge.update(projectPath    -> sum(TransformingTriples))
          ).sequence
        }
        .toList
        .sequence
        .void

    case DBUpdateResults.ForAllProjects =>
      List(awaitingGenerationGauge, underTriplesGenerationGauge, awaitingTransformationGauge, underTransformationGauge)
        .map(_.reset())
        .sequence
        .void
  }
}
