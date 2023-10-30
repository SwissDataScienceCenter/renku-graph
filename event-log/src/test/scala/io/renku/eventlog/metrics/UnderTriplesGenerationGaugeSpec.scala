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

package io.renku.eventlog.metrics

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonEmptySet, nonNegativeLongs}
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Slug
import io.renku.metrics._
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UnderTriplesGenerationGaugeSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "apply" should {

    "create and register events_processing_count named gauge" in new TestCase {

      (metricsRegistry
        .register(_: MetricsCollector with PrometheusCollector))
        .expects(*)
        .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[IO])

      val gauge = UnderTriplesGenerationGauge(statsFinder).unsafeRunSync()

      gauge.isInstanceOf[LabeledGauge[IO, projects.Slug]] shouldBe true
      gauge.name.value                                    shouldBe "events_under_triples_generation_count"
    }

    "return a gauge with reset method provisioning it with values from the Event Log" in new TestCase {

      (metricsRegistry
        .register(_: MetricsCollector with PrometheusCollector))
        .expects(*)
        .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[IO])

      val processingEvents = processingEventsGen.generateOne
      (statsFinder.countEvents _)
        .expects(Set(GeneratingTriples: EventStatus), None)
        .returning(processingEvents.pure[IO])

      UnderTriplesGenerationGauge(statsFinder).unsafeRunSync().reset().unsafeRunSync()
    }
  }

  private trait TestCase {
    implicit val metricsRegistry: MetricsRegistry[IO] = mock[MetricsRegistry[IO]]
    val statsFinder = mock[StatsFinder[IO]]
  }

  private lazy val processingEventsGen: Gen[Map[Slug, Long]] = nonEmptySet {
    (projectSlugs -> nonNegativeLongs().map(_.value)).mapN(_ -> _)
  }.map(_.toMap)
}
