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

package io.renku.eventlog.statuschange.commands

import java.time.Instant
import cats.effect.IO
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import doobie.implicits._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages, eventProcessingTimes, executionDates}
import io.renku.eventlog._
import io.renku.eventlog.statuschange.StatusUpdatesRunnerImpl
import org.http4s.Request
import org.http4s.circe.jsonEncoder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ToSkippedSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "command" should {

    s"set status $Skipped on the event with the given id and $GeneratingTriples status, " +
      "decrement under processing gauge for the project, insert the processingTime " +
      s"and return ${UpdateResult.Updated}" in new TestCase {

        storeEvent(
          compoundEventIds.generateOne.copy(id = eventId.id),
          EventStatus.GeneratingTriples,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate
        )
        val executionDate = executionDates.generateOne
        val projectPath   = projectPaths.generateOne
        storeEvent(
          eventId,
          EventStatus.GeneratingTriples,
          executionDate,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate,
          projectPath = projectPath
        )

        findEvent(eventId) shouldBe Some((executionDate, GeneratingTriples, None))

        (underTriplesGenerationGauge.decrement _).expects(projectPath).returning(IO.unit)

        val message = eventMessages.generateOne
        val command = ToSkipped[IO](eventId, message, underTriplesGenerationGauge, processingTime, currentTime)

        (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

        findEvent(eventId) shouldBe Some((ExecutionDate(now), Skipped, Some(message)))

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))
      }

    EventStatus.all.filterNot(_ == GeneratingTriples) foreach { eventStatus =>
      s"do nothing when updating event with $eventStatus status " +
        s"and return ${UpdateResult.NotFound}" in new TestCase {

          val executionDate = executionDates.generateOne
          storeEvent(eventId,
                     eventStatus,
                     executionDate,
                     eventDates.generateOne,
                     eventBodies.generateOne,
                     batchDate = eventBatchDate
          )

          findEvent(eventId) shouldBe Some((executionDate, eventStatus, None))

          val message = eventMessages.generateOne
          val command = ToSkipped[IO](eventId, message, underTriplesGenerationGauge, processingTime, currentTime)

          (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.NotFound

          findEvent(eventId)                       shouldBe Some((executionDate, eventStatus, None))
          findProcessingTime(eventId).eventIdsOnly shouldBe List()

          histogram.verifyExecutionTimeMeasured(command.queries.head.name)
        }
    }
    "factory" should {
      "decode properly a request" in new TestCase {
        val message             = eventMessages.generateOne
        val maybeProcessingTime = eventProcessingTimes.generateOption
        val expected =
          ToSkipped(eventId, message, underTriplesGenerationGauge, maybeProcessingTime)

        val body = json"""{
            "status": ${EventStatus.Skipped.value},
            "message": ${message.value}
          }""" deepMerge maybeProcessingTime
          .map(processingTime => json"""{"processingTime": ${processingTime.value.toString}  }""")
          .getOrElse(Json.obj())

        val request = Request[IO]().withEntity(body)

        val actual = ToSkipped
          .factory[IO](underTriplesGenerationGauge)
          .run((eventId, request))
        actual.unsafeRunSync() shouldBe Some(expected)
      }

      EventStatus.all.filterNot(status => status == Skipped) foreach { eventStatus =>
        s"return None if the decoding failed with status: $eventStatus " in new TestCase {
          val body =
            json"""{
              "status": ${eventStatus.value}
            }"""

          val request = Request[IO]().withEntity(body)

          val actual = ToSkipped.factory[IO](underTriplesGenerationGauge).run((eventId, request))

          actual.unsafeRunSync() shouldBe None
        }
      }
    }
  }

  private trait TestCase {
    val underTriplesGenerationGauge = mock[LabeledGauge[IO, projects.Path]]
    val histogram                   = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val currentTime                 = mockFunction[Instant]
    val eventId                     = compoundEventIds.generateOne
    val eventBatchDate              = batchDates.generateOne
    val processingTime              = eventProcessingTimes.generateSome

    val commandRunner = new StatusUpdatesRunnerImpl(transactor, histogram, TestLogger[IO]())

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
