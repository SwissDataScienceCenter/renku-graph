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

package io.renku.eventlog.metrics

import cats.effect.IO
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.GeneratingTriples
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{Gauge, LabeledGauge, MetricsRegistry}
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger

object UnderTriplesGenerationGauge {

  def apply(
      metricsRegistry: MetricsRegistry[IO],
      statsFinder:     StatsFinder[IO],
      logger:          Logger[IO]
  ): IO[LabeledGauge[IO, projects.Path]] =
    Gauge[IO, projects.Path](
      name = "events_under_triples_generation_count",
      help = "Number of Events under triples generation by project path.",
      labelName = "project",
      resetDataFetch =
        () => statsFinder.countEvents(Set(GeneratingTriples: EventStatus)).map(_.view.mapValues(_.toDouble).toMap)
    )(metricsRegistry)
}
