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

import cats.effect.IO
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.jsons
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages, executionDates}
import io.renku.eventlog._
import io.renku.eventlog.statuschange.StatusUpdatesRunnerImpl
import io.renku.eventlog.statuschange.commands.CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}
import org.http4s.circe.jsonEncoder
import org.http4s.headers.`Content-Type`
import org.http4s.{MediaType, Request}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class ToGenerationNonRecoverableFailureSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with ScalaCheckPropertyChecks
    with MockFactory
    with should.Matchers
    with TypeSerializers {

  "command" should {

    s"set status $GenerationNonRecoverableFailure on the event with the given id and $GeneratingTriples status" +
      "decrement waiting events and under processing gauges for the project, insert the processingTime" +
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

        val maybeMessage = Gen.option(eventMessages).generateOne
        val command = ToGenerationNonRecoverableFailure[IO](eventId,
                                                            maybeMessage,
                                                            underTriplesGenerationGauge,
                                                            processingTime,
                                                            currentTime
        )

        (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

        findEvent(eventId) shouldBe Some((ExecutionDate(now), GenerationNonRecoverableFailure, maybeMessage))

        findProcessingTime(eventId).eventIdsOnly shouldBe List(eventId)

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))
      }

    EventStatus.all.filterNot(status => status == GeneratingTriples) foreach { eventStatus =>
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

          val maybeMessage = Gen.option(eventMessages).generateOne
          val command =
            ToGenerationNonRecoverableFailure[IO](eventId,
                                                  maybeMessage,
                                                  underTriplesGenerationGauge,
                                                  processingTime,
                                                  currentTime
            )

          (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.NotFound

          findEvent(eventId)          shouldBe Some((executionDate, eventStatus, None))
          findProcessingTime(eventId) shouldBe List()

          histogram.verifyExecutionTimeMeasured(command.queries.head.name)
        }
    }

    "factory" should {
      "return a CommandFound when properly decoding a request" in new TestCase {
        val maybeMessage        = eventMessages.generateOption
        val maybeProcessingTime = eventProcessingTimes.generateOption
        val expected =
          ToGenerationNonRecoverableFailure(eventId, maybeMessage, underTriplesGenerationGauge, maybeProcessingTime)
        val body = json"""{
            "status": ${EventStatus.GenerationNonRecoverableFailure.value}
          }""" deepMerge maybeMessage
          .map(m => json"""{"message": ${m.value}}""")
          .getOrElse(Json.obj()) deepMerge maybeProcessingTime
          .map(processingTime => json"""{"processingTime": ${processingTime.value.toString}  }""")
          .getOrElse(Json.obj())

        val request = Request[IO]().withEntity(body)

        val actual = ToGenerationNonRecoverableFailure
          .factory[IO](underTriplesGenerationGauge)
          .run((eventId, request))
        actual.unsafeRunSync() shouldBe CommandFound(expected)
      }

      EventStatus.all.filterNot(status => status == GenerationNonRecoverableFailure) foreach { eventStatus =>
        s"return NotSupported if the decoding failed with status: $eventStatus " in new TestCase {
          val body =
            json"""{
              "status": ${eventStatus.value}
            }"""
          val request = Request[IO]().withEntity(body)

          val actual =
            ToGenerationNonRecoverableFailure
              .factory[IO](underTriplesGenerationGauge)
              .run((eventId, request))
          actual.unsafeRunSync() shouldBe NotSupported
        }
      }

      "return PayloadMalformed if the decoding failed because no status is present " in new TestCase {
        val body = json"""{ }"""

        val request = Request[IO]().withEntity(body)
        val actual = ToGenerationNonRecoverableFailure
          .factory[IO](underTriplesGenerationGauge)
          .run((eventId, request))

        actual.unsafeRunSync() shouldBe PayloadMalformed("No status property in status change payload")
      }

      "return NotSupported if the decoding failed because of unsupported content type " in new TestCase {
        val request =
          Request[IO]().withEntity(jsons.generateOne).withHeaders(`Content-Type`(MediaType.multipart.`form-data`))
        val actual =
          ToGenerationNonRecoverableFailure
            .factory[IO](underTriplesGenerationGauge)
            .run((eventId, request))

        actual.unsafeRunSync() shouldBe NotSupported
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
