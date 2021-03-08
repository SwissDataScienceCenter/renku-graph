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

package io.renku.eventlog.statuschange

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.{eventDates, executionDates}
import io.renku.eventlog.statuschange.commands.UpdateResult.{NotFound, Updated}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, ToTriplesStore, UpdateResult}
import io.renku.eventlog.subscriptions.EventDelivery
import io.renku.eventlog.{EventLogDB, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class StatusUpdatesRunnerSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "run" should {
    "return not found if the event is not in the DB" in new TestCase {

      val command = TestCommand(eventId, projectPath, gauge, eventDelivery = eventDelivery)

      (gauge.increment _).expects(projectPath).returning(IO.unit)

      runner.run(command).unsafeRunSync() shouldBe NotFound

    }

    "execute query from the given command, " +
      "map the result using command's result mapping rules, update the delivery status " +
      "and update metrics gauges" in new TestCase {

        store(eventId, projectPath, New)

        (gauge.increment _).expects(projectPath).returning(IO.unit)

        (eventDelivery.unregister _).expects(eventId).returning(IO.unit)

        val command = TestCommand(eventId, projectPath, gauge, eventDelivery = eventDelivery)

        runner.run(command).unsafeRunSync() shouldBe Updated

        findEvents(status = GeneratingTriples).eventIdsOnly shouldBe List(eventId)
        findProcessingTime(eventId).eventIdsOnly            shouldBe List(eventId)

        logger.loggedOnly(Info(s"Event $eventId got ${command.status}"))

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))
      }

    "execute query from the given command, " +
      "map the result using command's result mapping rules, log the error when the even delivery fails" +
      "and update metrics gauges" in new TestCase {

        store(eventId, projectPath, New)
        val exception = exceptions.generateOne

        (gauge.increment _).expects(projectPath).returning(IO.unit)

        val command = TestCommand(eventId, projectPath, gauge, eventDelivery = eventDelivery)

        (eventDelivery.unregister _).expects(eventId).returning(exception.raiseError[IO, Unit])

        runner.run(command).unsafeRunSync() shouldBe Updated

        findEvents(status = GeneratingTriples).eventIdsOnly shouldBe List(eventId)
        findProcessingTime(eventId).eventIdsOnly            shouldBe List(eventId)

        logger.logged(Info(s"Event $eventId got ${command.status}"),
                      Error(s"Event $eventId could not be updated in eventDelivery")
        )

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))
      }

    "execute query from the given command, " +
      "if the query fails rollback to the initial state" in new TestCase {

        store(eventId, projectPath, New)

        (gauge.increment _).expects(projectPath).returning(IO.unit)

        val command = TestFailingCommand(eventId,
                                         projectPath,
                                         gauge,
                                         eventProcessingTimes.generateSome,
                                         eventDelivery = eventDelivery
        )

        runner.run(command).unsafeRunSync() shouldBe NotFound

        findEvents(status = GeneratingTriples).eventIdsOnly shouldBe List()
        findProcessingTime(eventId).eventIdsOnly            shouldBe List()

        logger.loggedOnly(
          Info(
            s"${command.queries.head.name} failed for event ${command.eventId} to status ${command.status} with result $NotFound. Rolling back queries: ${command.queries.map(_.name.toString).toList.mkString(", ")}"
          )
        )

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))
      }
  }

  private trait TestCase {
    val eventId     = compoundEventIds.generateOne
    val projectPath = projectPaths.generateOne

    val gauge         = mock[LabeledGauge[IO, projects.Path]]
    val histogram     = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val logger        = TestLogger[IO]()
    val runner        = new StatusUpdatesRunnerImpl(transactor, histogram, logger)
    val eventDelivery = mock[EventDelivery[IO, ToTriplesStore[IO]]]
  }

  private case class TestCommand(eventId:             CompoundEventId,
                                 projectPath:         projects.Path,
                                 gauge:               LabeledGauge[IO, projects.Path],
                                 maybeProcessingTime: Option[EventProcessingTime] = eventProcessingTimes.generateSome,
                                 eventDelivery:       EventDelivery[IO, ToTriplesStore[IO]]
  ) extends ChangeStatusCommand[IO] {
    import doobie.implicits._

    override def status: EventStatus = GeneratingTriples

    override def queries = NonEmptyList(
      SqlQuery(
        sql"""|UPDATE event 
              |SET status = $status
              |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${New: EventStatus}
              |""".stripMargin.update.run,
        name = "test_status_update"
      ),
      Nil
    )

    override def mapResult: Int => UpdateResult = {
      case 0 => UpdateResult.NotFound
      case 1 => UpdateResult.Updated
      case _ => UpdateResult.Conflict
    }

    override def updateGauges(
        updateResult:      UpdateResult
    )(implicit transactor: DbTransactor[IO, EventLogDB]) = gauge increment projectPath

    override def updateDelivery(): IO[Unit] = eventDelivery.unregister(eventId)
  }

  private def store(eventId: CompoundEventId, projectPath: projects.Path, status: EventStatus): Unit =
    storeEvent(eventId,
               status,
               executionDates.generateOne,
               eventDates.generateOne,
               eventBodies.generateOne,
               projectPath = projectPath
    )

  private case class TestFailingCommand(eventId:             CompoundEventId,
                                        projectPath:         projects.Path,
                                        gauge:               LabeledGauge[IO, projects.Path],
                                        maybeProcessingTime: Option[EventProcessingTime],
                                        eventDelivery:       EventDelivery[IO, ToTriplesStore[IO]]
  ) extends ChangeStatusCommand[IO] {
    import doobie.implicits._

    override def status: EventStatus = GeneratingTriples

    override def queries = NonEmptyList(
      SqlQuery(
        sql"""|UPDATE event
              |SET status = $status
              |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${New: EventStatus}
              |""".stripMargin.update.run.map(_ => 0),
        name = "test_failure_status_update"
      ),
      Nil
    )

    override def mapResult: Int => UpdateResult = {
      case 0 => UpdateResult.NotFound
      case 1 => UpdateResult.Updated
      case _ => UpdateResult.Conflict
    }

    override def updateGauges(
        updateResult:      UpdateResult
    )(implicit transactor: DbTransactor[IO, EventLogDB]) = gauge increment projectPath

    override def updateDelivery(): IO[Unit] = eventDelivery.unregister(eventId)
  }

}
