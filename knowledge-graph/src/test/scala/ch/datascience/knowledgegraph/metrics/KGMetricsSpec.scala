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

package ch.datascience.knowledgegraph.metrics

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.Schemas.{prov, schema}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.metrics.LabeledGauge
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class KGMetricsSpec extends AnyWordSpec with MockFactory with Eventually with IntegrationPatience with should.Matchers {

  "run" should {

    "update the gauges with the fetched values" in new TestCase {

      val counts = groupingCountGen.generateOne
      (statsFinder.entitiesCount _)
        .expects()
        .returning(counts.pure[IO])
        .atLeastOnce()

      counts foreach { case (grouping, count) =>
        (countsGauge.set _)
          .expects(grouping -> count.value.toDouble)
          .returning(IO.unit)
          .atLeastOnce()
      }

      metrics.run().unsafeRunAsyncAndForget()

      sleep(1000)

      logger.expectNoLogs()
    }

    "log an eventual error and continue collecting the metrics" in new TestCase {
      val exception1 = exceptions.generateOne
      (statsFinder.entitiesCount _)
        .expects()
        .returning(exception1.raiseError[IO, Map[EntityLabel, Count]])

      val statuses = groupingCountGen.generateOne
      (statsFinder.entitiesCount _)
        .expects()
        .returning(statuses.pure[IO])
        .atLeastOnce()

      statuses foreach { case (entity, count) =>
        (countsGauge.set _)
          .expects(entity -> count.value.toDouble)
          .returning(IO.unit)
          .atLeastOnce()
      }

      metrics.run().start.unsafeRunAsyncAndForget()

      sleep(1000)

      eventually {
        logger.loggedOnly(Error("Problem with gathering metrics", exception1))
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestGauges {
    lazy val countsGauge = mock[LabeledGauge[IO, EntityLabel]]
  }

  private trait TestCase extends TestGauges {
    lazy val statsFinder = mock[StatsFinder[IO]]
    lazy val logger      = TestLogger[IO]()
    lazy val metrics = new KGMetricsImpl(
      statsFinder,
      logger,
      countsGauge,
      initialDelay = 100 millis,
      countsInterval = 500 millis
    )
  }

  private lazy val groupingCountGen: Gen[Map[EntityLabel, Count]] = nonEmptySet {
    for {
      entityType <- groupings
      count      <- nonNegativeLongs() map (long => Count(long.value))
    } yield entityType -> count
  }.map(_.toMap)

  private lazy val groupings: Gen[EntityLabel] = Gen
    .oneOf(
      schema / "Dataset",
      schema / "Project",
      prov / "Activity"
    )
    .map(objectType => EntityLabel(objectType.toString))
}
