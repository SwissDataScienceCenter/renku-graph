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

package io.renku.eventlog.metrics

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus.TransformingTriples
import io.renku.graph.model.projects
import io.renku.metrics.{Gauge, LabeledGauge, MetricsRegistry}

object UnderTransformationGauge {

  def apply(
      metricsRegistry: MetricsRegistry,
      statsFinder:     StatsFinder[IO]
  ): IO[LabeledGauge[IO, projects.Path]] =
    Gauge[IO, projects.Path](
      name = "events_under_transformation_count",
      help = "Number of Events under triples transformation by project path.",
      labelName = "project",
      resetDataFetch =
        () => statsFinder.countEvents(Set(TransformingTriples: EventStatus)).map(_.view.mapValues(_.toDouble).toMap)
    )(metricsRegistry)
}
