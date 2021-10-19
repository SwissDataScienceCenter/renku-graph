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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.{CategoryName, EventStatus}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.metrics.{LabeledGauge, SingleValueGauge}
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

      val categoryNameCount = categoryNameCounts.generateOne
      givenCountEventsByCategoryNameMethodToReturn add categoryNameCount.pure[IO]

      val statusCount = statusCounts.generateOne
      givenStatusesMethodToReturn add statusCount.pure[IO]

      metrics.run().unsafeRunAsyncAndForget()

      eventually {
        categoryNameEventsGauge.categoryNameValues.asScala shouldBe categoryNameCount
        statusesGauge.statusValues.asScala                 shouldBe statusCount
        totalGauge.values.asScala.toList.filter(_ > 0d)    shouldBe List(statusCount.valuesIterator.sum.toDouble)
      }

      logger.expectNoLogs()
    }

    "log an eventual error and continue collecting the metrics" in new TestCase {

      val categoryNamesException = exceptions.generateOne
      givenCountEventsByCategoryNameMethodToReturn add categoryNamesException.raiseError[IO, Map[CategoryName, Long]]
      val categoryNameCount = categoryNameCounts.generateOne
      givenCountEventsByCategoryNameMethodToReturn add categoryNameCount.pure[IO]

      val statusesFindException = exceptions.generateOne
      givenStatusesMethodToReturn add statusesFindException.raiseError[IO, Map[EventStatus, Long]]
      val statuses = statusCounts.generateOne
      givenStatusesMethodToReturn add statuses.pure[IO]

      metrics.run().unsafeRunAsyncAndForget()

      eventually {
        categoryNameEventsGauge.categoryNameValues.asScala shouldBe categoryNameCount
        statusesGauge.statusValues.asScala                 shouldBe statuses
        totalGauge.values.asScala.toList.filter(_ > 0d)    shouldBe List(statuses.valuesIterator.sum.toDouble)

        logger.loggedOnly(
          Error(s"Problem with gathering metrics for ${categoryNameEventsGauge.name}", categoryNamesException),
          Error(s"Problem with gathering metrics for ${statusesGauge.name}", statusesFindException)
        )
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestGauges {

    lazy val categoryNameEventsGauge = new LabeledGauge[IO, CategoryName] {
      val categoryNameValues = new ConcurrentHashMap[CategoryName, Double]()

      override lazy val name = "category name gauge"

      override def set(labelValue: (CategoryName, Double)) = labelValue match {
        case (categoryName, value) => categoryNameValues.put(categoryName, value).pure[IO].void
      }

      override def update(labelValue: (CategoryName, Double)) = fail("Spec shouldn't be calling that")
      override def increment(labelValue: CategoryName)        = fail("Spec shouldn't be calling that")
      override def decrement(labelValue: CategoryName)        = fail("Spec shouldn't be calling that")
      override def reset()                                    = fail("Spec shouldn't be calling that")
      protected override def gauge                            = fail("Spec shouldn't be calling that")
    }

    lazy val statusesGauge = new LabeledGauge[IO, EventStatus] {
      val statusValues = new ConcurrentHashMap[EventStatus, Double]()

      override lazy val name = "status gauge"

      override def set(labelValue: (EventStatus, Double)) = labelValue match {
        case (status, value) => statusValues.put(status, value).pure[IO].void
      }

      override def update(labelValue: (EventStatus, Double)) = fail("Spec shouldn't be calling that")
      override def increment(labelValue: EventStatus)        = fail("Spec shouldn't be calling that")
      override def decrement(labelValue: EventStatus)        = fail("Spec shouldn't be calling that")
      override def reset()                                   = fail("Spec shouldn't be calling that")
      protected override def gauge                           = fail("Spec shouldn't be calling that")
    }

    lazy val totalGauge = new SingleValueGauge[IO] {
      val values                      = new ConcurrentLinkedQueue[Double]()
      override def set(value: Double) = values.add(value).pure[IO].void
      protected override def gauge    = fail("Spec shouldn't be calling that")
    }
  }

  private trait TestCase extends TestGauges {
    val givenStatusesMethodToReturn                  = new ConcurrentLinkedQueue[IO[Map[EventStatus, Long]]]()
    val givenCountEventsByCategoryNameMethodToReturn = new ConcurrentLinkedQueue[IO[Map[CategoryName, Long]]]()

    lazy val statsFinder = new StatsFinder[IO] {
      override def statuses(): IO[Map[EventStatus, Long]] =
        Option(givenStatusesMethodToReturn.poll()) getOrElse Map.empty[EventStatus, Long].pure[IO]

      override def countEventsByCategoryName(): IO[Map[CategoryName, Long]] =
        Option(givenCountEventsByCategoryNameMethodToReturn.poll()) getOrElse Map.empty[CategoryName, Long].pure[IO]

      override def countEvents(statuses: Set[EventStatus], maybeLimit: Option[Refined[Int, Positive]]) =
        fail("Spec shouldn't be calling that")
    }
    implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
    lazy val metrics = new EventLogMetricsImpl(
      statsFinder,
      categoryNameEventsGauge,
      statusesGauge,
      totalGauge,
      interval = 500 millis
    )
  }

  private lazy val statusCounts: Gen[Map[EventStatus, Long]] = nonEmptySet {
    for {
      status <- eventStatuses
      count  <- nonNegativeLongs()
    } yield status -> count.value
  }.map(_.toMap)

  private lazy val categoryNameCounts: Gen[Map[CategoryName, Long]] = nonEmptySet {
    for {
      categoryName <- categoryNames
      count        <- nonNegativeLongs()
    } yield categoryName -> count.value
  }.map(_.toMap)
}
