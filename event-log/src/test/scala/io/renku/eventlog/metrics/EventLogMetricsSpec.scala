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

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.metrics.{LabeledGauge, SingleValueGauge}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.{postfixOps, reflectiveCalls}

class EventLogMetricsSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  "run" should {

    "update the gauges with the fetched values" in new TestCase {

      val statuses = statuesGen.generateOne
      givenStatusesMethodToReturn add statuses.pure[IO]

      metrics.run().unsafeRunAsyncAndForget()

      eventually {
        statusesGauge.actualStatuses.asScala                  shouldBe statuses
        totalGauge.actualValues.asScala.toList.filter(_ > 0d) shouldBe List(statuses.valuesIterator.sum.toDouble)
      }

      logger.expectNoLogs()
    }

    "log an eventual error and continue collecting the metrics" in new TestCase {

      val exception = exceptions.generateOne
      givenStatusesMethodToReturn.add(exception.raiseError[IO, Map[EventStatus, Long]])
      val statuses = statuesGen.generateOne
      givenStatusesMethodToReturn.add(statuses.pure[IO])

      metrics.run().unsafeRunAsyncAndForget()

      eventually {
        statusesGauge.actualStatuses.asScala                  shouldBe statuses
        totalGauge.actualValues.asScala.toList.filter(_ > 0d) shouldBe List(statuses.valuesIterator.sum.toDouble)

        logger.loggedOnly(Error("Problem with gathering metrics", exception))
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestGauges {

    lazy val statusesGauge = new LabeledGauge[IO, EventStatus] {
      val actualStatuses = new ConcurrentHashMap[EventStatus, Double]()

      override def set(labelValue: (EventStatus, Double)) = labelValue match {
        case (status, value) => actualStatuses.put(status, value).pure[IO].void
      }

      override def increment(labelValue: EventStatus) = fail("Spec shouldn't be calling that")
      override def decrement(labelValue: EventStatus) = fail("Spec shouldn't be calling that")
      override def reset()         = fail("Spec shouldn't be calling that")
      protected override def gauge = fail("Spec shouldn't be calling that")
    }

    lazy val totalGauge = new SingleValueGauge[IO] {
      val actualValues = new ConcurrentLinkedQueue[Double]()
      override def set(value: Double) = actualValues.add(value).pure[IO].void
      protected override def gauge = fail("Spec shouldn't be calling that")
    }
  }

  private trait TestCase extends TestGauges {
    val givenStatusesMethodToReturn = new ConcurrentLinkedQueue[IO[Map[EventStatus, Long]]]()

    lazy val statsFinder = new StatsFinder[IO] {
      override def statuses(): IO[Map[EventStatus, Long]] =
        Option(givenStatusesMethodToReturn.poll()) getOrElse Map.empty[EventStatus, Long].pure[IO]

      override def countEvents(statuses: Set[EventStatus], maybeLimit: Option[Refined[Int, Positive]]) =
        fail("Spec shouldn't be calling that")
    }
    lazy val logger = TestLogger[IO]()
    lazy val metrics = new EventLogMetrics(
      statsFinder,
      logger,
      statusesGauge,
      totalGauge,
      interval = 500 millis
    )
  }

  private lazy val statuesGen: Gen[Map[EventStatus, Long]] = nonEmptySet {
    for {
      status <- eventStatuses
      count  <- nonNegativeLongs()
    } yield status -> count.value
  }.map(_.toMap)
}
