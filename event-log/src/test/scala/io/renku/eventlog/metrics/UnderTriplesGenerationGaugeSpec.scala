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

import cats.MonadError
import cats.effect.IO
import cats.syntax.all._
import io.prometheus.client.{Gauge => LibGauge}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonEmptySet, nonNegativeLongs}
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Path
import io.renku.metrics.MetricsTools._
import io.renku.metrics.{LabeledGauge, MetricsRegistry}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class UnderTriplesGenerationGaugeSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "apply" should {

    "create and register events_processing_count named gauge" in new TestCase {

      (metricsRegistry
        .register[IO, LibGauge, LibGauge.Builder](_: LibGauge.Builder)(_: MonadError[IO, Throwable]))
        .expects(*, *)
        .onCall { (builder: LibGauge.Builder, _: MonadError[IO, Throwable]) =>
          val actual = builder.create()
          actual.describe().asScala.head.name shouldBe underlying.describe().asScala.head.name
          actual.describe().asScala.head.help shouldBe underlying.describe().asScala.head.help
          actual.pure[IO]
        }

      val gauge = UnderTriplesGenerationGauge(metricsRegistry, statsFinder).unsafeRunSync()

      gauge.isInstanceOf[LabeledGauge[IO, projects.Path]] shouldBe true
    }

    "return a gauge with reset method provisioning it with values from the Event Log" in new TestCase {

      (metricsRegistry
        .register[IO, LibGauge, LibGauge.Builder](_: LibGauge.Builder)(_: MonadError[IO, Throwable]))
        .expects(*, *)
        .onCall((_: LibGauge.Builder, _: MonadError[IO, Throwable]) => underlying.pure[IO])

      val processingEvents = processingEventsGen.generateOne
      (statsFinder.countEvents _)
        .expects(Set(GeneratingTriples: EventStatus), None)
        .returning(processingEvents.pure[IO])

      UnderTriplesGenerationGauge(metricsRegistry, statsFinder).flatMap(_.reset()).unsafeRunSync()

      underlying.collectAllSamples should contain theSameElementsAs processingEvents.map { case (project, count) =>
        ("project", project.value, count.toDouble)
      }
    }
  }

  private trait TestCase {
    val underlying = LibGauge
      .build("events_under_triples_generation_count", "Number of Events under triples generation by project path.")
      .labelNames("project")
      .create()

    val metricsRegistry = mock[MetricsRegistry]
    val statsFinder     = mock[StatsFinder[IO]]
  }

  private lazy val processingEventsGen: Gen[Map[Path, Long]] = nonEmptySet {
    for {
      path  <- projectPaths
      count <- nonNegativeLongs()
    } yield path -> count.value
  }.map(_.toMap)
}
