/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.metrics

import java.util.concurrent.ConcurrentLinkedQueue

import EventLogMetrics._
import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.commands.EventLogStats
import ch.datascience.dbeventlog.{EventLogDB, EventStatus}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.interpreters.{TestDbTransactor, TestLogger}
import ch.datascience.metrics.MetricsRegistry
import io.prometheus.client.Gauge
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}

class EventLogMetricsSpec extends WordSpec with MockFactory with Eventually with IntegrationPatience {

  "run" should {

    "update the gauges with the fetched values" in new TestCase {

      val waitingEvents = waitingEventsGen.generateOne
      (eventLogStats.waitingEvents _)
        .expects()
        .returning(waitingEvents.pure[IO])
        .atLeastOnce()

      val statuses = statuesGen.generateOne
      (eventLogStats.statuses _)
        .expects()
        .returning(statuses.pure[IO])
        .atLeastOnce()

      metrics.run.start.unsafeRunCancelable(_ => ())

      eventually {
        waitingEvents foreach {
          case (path, count) =>
            waitingEventsGauge.labels(path.toString).get().toLong shouldBe count
        }
      }

      eventually {
        statuses foreach {
          case (status, count) =>
            statusesGauge.labels(status.toString).get().toLong shouldBe count
        }
      }

      eventually {
        totalGauge.get().toLong shouldBe statuses.valuesIterator.sum
      }
    }

    "update the waitingEventsGauge with all the data just during the first update; " +
      "next updates should contain non-zero values and zeros only for projects having non-zeros before" in new TestCase {

      val project1           = projectPaths.generateOne
      val project2           = projectPaths.generateOne
      val project3           = projectPaths.generateOne
      val waitingEventsCall1 = Map(project1 -> positiveLongs().generateOne.value, project2 -> 0L, project3 -> 0L)
      val waitingEventsCall2 = Map(project1 -> positiveLongs().generateOne.value,
                                   project2 -> 0L,
                                   project3 -> positiveLongs().generateOne.value)
      val waitingEventsCall3 = Map(project1 -> 0L, project2 -> 0L, project3 -> positiveLongs().generateOne.value)

      override lazy val eventLogStats: EventLogStats[IO] = new EventLogStats[IO] {

        override def statuses: IO[Map[EventStatus, Long]] = statuesGen.generateOne.pure[IO]

        private val waitingEventsQueue = new ConcurrentLinkedQueue(
          List(waitingEventsCall1, waitingEventsCall2, waitingEventsCall3).asJava
        )

        override def waitingEvents: IO[Map[ProjectPath, Long]] =
          Option(waitingEventsQueue.poll())
            .getOrElse(waitingEventsCall3)
            .pure[IO]
      }

      metrics.run.start.unsafeRunCancelable(_ => ())

      eventually {
        waitingEventsGauge.labels(project1.toString).get().toLong shouldBe waitingEventsCall1(project1)
        waitingEventsGauge.labels(project2.toString).get().toLong shouldBe waitingEventsCall1(project2)
        waitingEventsGauge.labels(project3.toString).get().toLong shouldBe waitingEventsCall1(project3)
      }

      eventually {
        waitingEventsGauge.collect.asScala
          .flatMap(_.samples.asScala)
          .flatMap(_.labelValues.asScala) should contain only (project1.toString, project3.toString)

        waitingEventsGauge.labels(project1.toString).get().toLong shouldBe waitingEventsCall2(project1)
        waitingEventsGauge.labels(project3.toString).get().toLong shouldBe waitingEventsCall2(project3)
      }

      eventually {
        waitingEventsGauge.collect.asScala
          .flatMap(_.samples.asScala)
          .flatMap(_.labelValues.asScala) should contain only project3.toString

        waitingEventsGauge.labels(project3.toString).get().toLong shouldBe waitingEventsCall3(project3)
      }
    }

    "log an eventual error and continue collecting the metrics" in new TestCase {
      val exception1 = exceptions.generateOne
      (eventLogStats.statuses _)
        .expects()
        .returning(exception1.raiseError[IO, Map[EventStatus, Long]])
      val exception2 = exceptions.generateOne
      (eventLogStats.waitingEvents _)
        .expects()
        .returning(exception2.raiseError[IO, Map[ProjectPath, Long]])
      val statuses = statuesGen.generateOne
      (eventLogStats.statuses _)
        .expects()
        .returning(statuses.pure[IO])
        .atLeastOnce()
      val waitingEvents = waitingEventsGen.generateOne
      (eventLogStats.waitingEvents _)
        .expects()
        .returning(waitingEvents.pure[IO])
        .atLeastOnce()

      metrics.run.start.unsafeRunCancelable(_ => ())

      eventually {
        waitingEvents foreach {
          case (path, count) =>
            waitingEventsGauge.labels(path.toString).get().toLong shouldBe count
        }
      }

      eventually {
        statuses foreach {
          case (status, count) =>
            statusesGauge.labels(status.toString).get().toLong shouldBe count
        }
      }

      eventually {
        totalGauge.get().toLong shouldBe statuses.valuesIterator.sum
      }

      eventually {
        logger.loggedOnly(Error("Problem with gathering metrics", exception1),
                          Error("Problem with gathering metrics", exception2))
      }
    }
  }

  "apply" should {

    "register the metrics in the Metrics Registry" in new TestGauges {

      val metricsRegistry = mock[MetricsRegistry[IO]]

      (metricsRegistry
        .register[Gauge, Gauge.Builder](_: Gauge.Builder)(_: MonadError[IO, Throwable]))
        .expects(waitingEventsGaugeBuilder, *)
        .returning(IO.pure(waitingEventsGauge))

      (metricsRegistry
        .register[Gauge, Gauge.Builder](_: Gauge.Builder)(_: MonadError[IO, Throwable]))
        .expects(statusesGaugeBuilder, *)
        .returning(IO.pure(statusesGauge))

      (metricsRegistry
        .register[Gauge, Gauge.Builder](_: Gauge.Builder)(_: MonadError[IO, Throwable]))
        .expects(totalGaugeBuilder, *)
        .returning(IO.pure(totalGauge))

      IOEventLogMetrics(mock[TestDbTransactor[EventLogDB]], TestLogger[IO](), metricsRegistry)
        .unsafeRunSync()
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestGauges {
    lazy val waitingEventsGauge = waitingEventsGaugeBuilder.create()
    lazy val statusesGauge      = statusesGaugeBuilder.create()
    lazy val totalGauge         = totalGaugeBuilder.create()
  }

  private trait TestCase extends TestGauges {
    lazy val eventLogStats: EventLogStats[IO] = mock[EventLogStats[IO]]
    lazy val logger = TestLogger[IO]()
    lazy val metrics = new EventLogMetrics(
      eventLogStats,
      logger,
      waitingEventsGauge,
      statusesGauge,
      totalGauge,
      interval              = 100 millis,
      statusesInterval      = 100 millis,
      waitingEventsInterval = 500 millis
    )
  }

  private lazy val statuesGen: Gen[Map[EventStatus, Long]] = nonEmptySet {
    for {
      status <- eventStatuses
      count  <- nonNegativeLongs()
    } yield status -> count.value
  }.map(_.toMap)

  private lazy val waitingEventsGen: Gen[Map[ProjectPath, Long]] = nonEmptySet {
    for {
      path  <- projectPaths
      count <- nonNegativeLongs()
    } yield path -> count.value
  }.map(_.toMap)
}
