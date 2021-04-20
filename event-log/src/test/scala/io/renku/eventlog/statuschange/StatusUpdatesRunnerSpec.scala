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

import cats.data.{Kleisli, NonEmptyList}
import cats.syntax.all._
import cats.effect.IO
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.{eventDates, executionDates}
import io.renku.eventlog.statuschange.commands.UpdateResult.{NotFound, Updated}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}
import io.renku.eventlog.{EventLogDB, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.data.Completion
import skunk.implicits._

class StatusUpdatesRunnerSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "run" should {

    "return not found if the event is not in the DB" in new TestCase {

      val command = TestCommand(eventId, projectPath, gauge)

      (gauge.increment _).expects(projectPath).returning(IO.unit)

      runner.run(command).unsafeRunSync() shouldBe NotFound
    }

    "remove the delivery info " +
      "execute query from the given command, " +
      "map the result using command's result mapping rules, " +
      "and update metrics gauges" in new TestCase {

        store(eventId, projectPath, New)
        upsertEventDelivery(eventId)

        (gauge.increment _).expects(projectPath).returning(IO.unit)

        val command = TestCommand(eventId, projectPath, gauge)

        runner.run(command).unsafeRunSync() shouldBe Updated

        findEvents(status = GeneratingTriples).eventIdsOnly shouldBe List(eventId)
        findProcessingTime(eventId).eventIdsOnly            shouldBe List(eventId)

        logger.loggedOnly(Info(s"Event $eventId got ${command.status}"))

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))

        findAllDeliveries shouldBe Nil
      }

    "execute query from the given command, " +
      "do nothing if event delivery for the event does not exist, " +
      "map the result using command's result mapping rules, " +
      "and update metrics gauges" in new TestCase {

        store(eventId, projectPath, New)

        (gauge.increment _).expects(projectPath).returning(IO.unit)

        val command = TestCommand(eventId, projectPath, gauge)

        runner.run(command).unsafeRunSync() shouldBe Updated

        findEvents(status = GeneratingTriples).eventIdsOnly shouldBe List(eventId)
        findProcessingTime(eventId).eventIdsOnly            shouldBe List(eventId)

        logger.logged(Info(s"Event $eventId got ${command.status}"))

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))

        findAllDeliveries shouldBe Nil
      }

    "execute query from the given command, " +
      "if the query fails rollback to the initial state " +
      "except from the event delivery info" in new TestCase {

        store(eventId, projectPath, New)
        upsertEventDelivery(eventId)

        (gauge.increment _).expects(projectPath).returning(IO.unit)

        val command = TestFailingCommand(eventId, projectPath, gauge, eventProcessingTimes.generateSome)

        val UpdateResult.Failure(message) = runner.run(command).unsafeRunSync()

        message.value shouldBe s"${command.queries.head.name} failed for event $eventId " +
          s"to status ${command.status} with result ${command.queryResult}. " +
          s"Rolling back queries: ${List(command.queries.head.name.value, "upsert_processing_time")
            .mkString(", ")}"

        findEvents(status = New).eventIdsOnly               shouldBe List(eventId)
        findEvents(status = GeneratingTriples).eventIdsOnly shouldBe List()
        findProcessingTime(eventId).eventIdsOnly            shouldBe List()

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))

        findAllDeliveries.map(_._1) shouldBe Nil
      }
  }

  private trait TestCase {
    val eventId     = compoundEventIds.generateOne
    val projectPath = projectPaths.generateOne

    val gauge     = mock[LabeledGauge[IO, projects.Path]]
    val histogram = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val logger    = TestLogger[IO]()
    val runner    = new StatusUpdatesRunnerImpl(sessionResource, histogram, logger)
  }

  private case class TestCommand(eventId:             CompoundEventId,
                                 projectPath:         projects.Path,
                                 gauge:               LabeledGauge[IO, projects.Path],
                                 maybeProcessingTime: Option[EventProcessingTime] = eventProcessingTimes.generateSome
  ) extends ChangeStatusCommand[IO] {

    override def status: EventStatus = GeneratingTriples

    override def queries = NonEmptyList(
      SqlQuery(
        Kleisli { session =>
          val query: Command[EventStatus ~ EventId ~ projects.Id ~ EventStatus] = sql"""
            UPDATE event
            SET status = $eventStatusPut
            WHERE event_id = $eventIdPut AND project_id = $projectIdPut AND status = $eventStatusPut
            """.command
          session.prepare(query).use(_.execute(status ~ eventId.id ~ eventId.projectId ~ New)).map {
            case Completion.Update(n) => n
            case completion =>
              throw new RuntimeException(
                s"generating_triples->generation_non_recoverable_fail time query failed with completion status $completion"
              )
          }
        },
        name = "test_status_update"
      ),
      Nil
    )

    override def updateGauges(
        updateResult: UpdateResult
    ): Kleisli[IO, Session[IO], Unit] = Kleisli.liftF(gauge increment projectPath)
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
                                        maybeProcessingTime: Option[EventProcessingTime]
  ) extends ChangeStatusCommand[IO] {

    override def status: EventStatus = GeneratingTriples

    val queryResult: Int = 0

    override def queries = NonEmptyList.of(
      SqlQuery(Kleisli(_ => 0.pure[IO]), name = "test_failure_status_update")
    )

    override def updateGauges(
        updateResult: UpdateResult
    ): Kleisli[IO, Session[IO], Unit] = Kleisli.liftF(gauge increment projectPath)
  }
}
