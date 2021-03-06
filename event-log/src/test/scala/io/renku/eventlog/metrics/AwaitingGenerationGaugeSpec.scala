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

import cats.MonadError
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{nonEmptySet, nonNegativeLongs}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import ch.datascience.metrics.MetricsTools._
import ch.datascience.metrics.{LabeledGauge, MetricsRegistry}
import io.prometheus.client.{Gauge => LibGauge}
import io.renku.eventlog.metrics.AwaitingGenerationGauge.NumberOfProjects
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class AwaitingGenerationGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "apply" should {

    "create and register an events_awaiting_generation_count named gauge" in new TestCase {

      (metricsRegistry
        .register[LibGauge, LibGauge.Builder](_: LibGauge.Builder)(_: MonadError[IO, Throwable]))
        .expects(*, *)
        .onCall { (builder: LibGauge.Builder, _: MonadError[IO, Throwable]) =>
          val actual = builder.create()
          actual.describe().asScala.head.name shouldBe underlying.describe().asScala.head.name
          actual.describe().asScala.head.help shouldBe underlying.describe().asScala.head.help
          actual.pure[IO]
        }

      val gauge = AwaitingGenerationGauge(metricsRegistry, statsFinder).unsafeRunSync()

      gauge.isInstanceOf[LabeledGauge[IO, projects.Path]] shouldBe true
    }

    "return a gauge with reset method provisioning it with values from the Event Log" in new TestCase {

      (metricsRegistry
        .register[LibGauge, LibGauge.Builder](_: LibGauge.Builder)(_: MonadError[IO, Throwable]))
        .expects(*, *)
        .onCall((_: LibGauge.Builder, _: MonadError[IO, Throwable]) => underlying.pure[IO])

      val waitingEvents = waitingEventsGen.generateOne
      (statsFinder.countEvents _)
        .expects(Set(New, GenerationRecoverableFailure), Some(NumberOfProjects))
        .returning(waitingEvents.pure[IO])

      AwaitingGenerationGauge(metricsRegistry, statsFinder).flatMap(_.reset()).unsafeRunSync()

      underlying.collectAllSamples should contain theSameElementsAs waitingEvents.map { case (project, count) =>
        ("project", project.value, count.toDouble)
      }
    }
  }

  private trait TestCase {
    val underlying = LibGauge
      .build("events_awaiting_generation_count",
             "Number of Events waiting to have their triples generated by project path."
      )
      .labelNames("project")
      .create()

    val metricsRegistry = mock[MetricsRegistry[IO]]
    val statsFinder     = mock[StatsFinder[IO]]
  }

  private lazy val waitingEventsGen: Gen[Map[Path, Long]] = nonEmptySet {
    for {
      path  <- projectPaths
      count <- nonNegativeLongs()
    } yield path -> count.value
  }.map(_.toMap)
}
