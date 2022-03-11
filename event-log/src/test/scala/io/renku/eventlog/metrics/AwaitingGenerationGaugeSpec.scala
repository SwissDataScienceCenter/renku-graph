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
import cats.syntax.all._
import io.prometheus.client.{Gauge => LibGauge}
import io.renku.eventlog.metrics.AwaitingGenerationGauge.NumberOfProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonEmptySet, nonNegativeLongs}
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Path
import io.renku.metrics.MetricsTools._
import io.renku.metrics._
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class AwaitingGenerationGaugeSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "apply" should {

    "create and register an events_awaiting_generation_count named gauge" in new TestCase {

      (metricsRegistry
        .register(_: MetricsCollector with PrometheusCollector))
        .expects(where[MetricsCollector with PrometheusCollector](_.wrappedCollector.isInstanceOf[LibGauge]))
        .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[IO])

      val gauge = AwaitingGenerationGauge(statsFinder).unsafeRunSync()

      gauge.isInstanceOf[LabeledGauge[IO, projects.Path]] shouldBe true
      gauge.name                                          shouldBe "events_awaiting_generation_count"
    }

    "return a gauge with reset method provisioning it with values from the Event Log" in new TestCase {

      (metricsRegistry
        .register(_: MetricsCollector with PrometheusCollector))
        .expects(*)
        .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[IO])

      val waitingEvents = waitingEventsGen.generateOne
      (statsFinder.countEvents _)
        .expects(Set(New, GenerationRecoverableFailure), Some(NumberOfProjects))
        .returning(waitingEvents.pure[IO])

      val gauge = AwaitingGenerationGauge(statsFinder).unsafeRunSync()

      gauge.reset().unsafeRunSync()

      gauge
        .asInstanceOf[LabeledGaugeImpl[IO, projects.Path]]
        .wrappedCollector
        .collectAllSamples should contain theSameElementsAs waitingEvents.map { case (project, count) =>
        ("project", project.value, count.toDouble)
      }
    }
  }

  private trait TestCase {
    implicit val metricsRegistry: MetricsRegistry[IO] = mock[MetricsRegistry[IO]]
    val statsFinder = mock[StatsFinder[IO]]
  }

  private lazy val waitingEventsGen: Gen[Map[Path, Long]] = nonEmptySet {
    for {
      path  <- projectPaths
      count <- nonNegativeLongs()
    } yield path -> count.value
  }.map(_.toMap)
}
