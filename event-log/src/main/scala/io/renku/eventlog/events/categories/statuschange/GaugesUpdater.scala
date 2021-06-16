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

import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, GenerationRecoverableFailure, New, TransformationRecoverableFailure, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge

private trait GaugesUpdater[Interpretation[_]] {
  def updateGauges(dbUpdateResults: DBUpdateResults): Interpretation[Unit]
}

private class GaugesUpdaterImpl[Interpretation[_]](
    awaitingGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    underTransformationGauge:    LabeledGauge[Interpretation, projects.Path],
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path]
) extends GaugesUpdater[Interpretation] {

  override def updateGauges(dbUpdateResults: DBUpdateResults): Interpretation[Unit] = {
    import dbUpdateResults._

    def sumCounts(of: EventStatus*): Double =
      changedStatusCounts.view.filterKeys(of.contains).values.sum

    awaitingGenerationGauge.update(projectPath     -> -sumCounts(New, GenerationRecoverableFailure))
    underTriplesGenerationGauge.update(projectPath -> -sumCounts(GeneratingTriples))
    awaitingTransformationGauge.update(projectPath -> -sumCounts(TriplesGenerated, TransformationRecoverableFailure))
    underTransformationGauge.update(projectPath    -> -sumCounts(TransformingTriples))
  }
}
