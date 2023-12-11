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

package io.renku.eventlog.events.consumers
package statuschange

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.circe.syntax._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent._
import io.renku.eventlog.api.events.{StatusChangeEvent, StatusChangeGenerators}
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.{EventSchedulingResult, ProcessExecutor}
import io.renku.events.producers.EventSender
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.metrics.{MetricsRegistry, TestMetricsRegistry}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EventHandlerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with EitherValues
    with should.Matchers
    with ScalaCheckPropertyChecks {

  "createHandlingDefinition.decode" should {

    "decode valid event data" in testDBResource.use { implicit cfg =>
      val definition = handler().createHandlingDefinition()
      forAll(StatusChangeGenerators.statusChangeEvents) { event =>
        val decoded = definition.decode(eventRequestContent(event))
        decoded shouldBe Right(event)
      }.pure[IO]
    }

    "fail if no payload exist for TriplesGenerated" in testDBResource.use { implicit cfg =>
      val definition = handler().createHandlingDefinition()
      forAll(StatusChangeGenerators.toTriplesGeneratedEvents) { event =>
        val req = EventRequestContent(event.asJson)
        definition.decode(req).left.value.getMessage should endWith(show"Missing event payload for: $event")
      }.pure[IO]
    }
  }

  "createHandlingDefinition.process" should {

    "call to StatusChanger" in testDBResource.use { implicit cfg =>
      val event = StatusChangeGenerators.statusChangeEvents.generateOne
      givenStatusChanging(event, returning = IO.unit)

      handler().createHandlingDefinition().process(event).assertNoException
    }

    "not log rollback events when Accepted" in testDBResource.use { implicit cfg =>
      val event = StatusChangeGenerators.rollbackEvents.generateOne
      givenStatusChanging(event, returning = IO.unit)

      logger.resetF() >>
        handler().tryHandling(eventRequestContent(event)).assertNoException >>
        logger.expectNoLogsF()
    }

    "log other events when Accepted" in testDBResource.use { implicit cfg =>
      val event = StatusChangeGenerators.nonRollbackEvents.generateOne
      givenStatusChanging(event, returning = IO.unit)

      handler().tryHandling(eventRequestContent(event)).assertNoException >>
        logger.loggedF(Info(show"$categoryName: $event -> ${EventSchedulingResult.Accepted}"))
    }

    "log on scheduling error" in testDBResource.use { implicit cfg =>
      val error           = new Exception("fail")
      val result          = EventSchedulingResult.SchedulingError(error)
      val processExecutor = ProcessExecutor[IO](_ => IO.pure(result))
      val event           = StatusChangeGenerators.statusChangeEvents.generateOne

      handler(processExecutor).tryHandling(eventRequestContent(event)).asserting(_ shouldBe result) >>
        logger.loggedF(Error(show"$categoryName: $event -> $result", error))
    }
  }

  "createHandlingDefinition" should {
    "not define onRelease and precondition" in testDBResource.use { implicit cfg =>
      val definition = handler().createHandlingDefinition()
      definition.onRelease shouldBe None
      definition.precondition.asserting(_ shouldBe None)
    }
  }

  private lazy val statusChanger  = mock[StatusChanger[IO]]
  private val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
  private val eventsQueue         = mock[StatusChangeEventsQueue[IO]]
  private val eventSender         = mock[EventSender[IO]]

  val processExecutor: ProcessExecutor[IO] = ProcessExecutor.sequential

  private def handler(processExecutor: ProcessExecutor[IO] = processExecutor)(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val metricsRegistry: MetricsRegistry[IO]       = TestMetricsRegistry[IO]
    implicit val qet:             QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventHandler[IO](processExecutor, statusChanger, eventSender, eventsQueue, deliveryInfoRemover)
  }

  private def givenStatusChanging(event: StatusChangeEvent, returning: IO[Unit]) =
    (statusChanger
      .updateStatuses(_: DBUpdater[IO, StatusChangeEvent])(_: StatusChangeEvent))
      .expects(*, event)
      .returning(returning)

  private def eventRequestContent(event: StatusChangeEvent): EventRequestContent = event match {
    case e: StatusChangeEvent.ToTriplesGenerated =>
      EventRequestContent.WithPayload(event.asJson, e.payload)
    case _ => EventRequestContent.NoPayload(event.asJson)
  }
}
