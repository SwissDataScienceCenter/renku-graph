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

package io.renku.eventlog.init

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.{EventBody, EventId, EventStatus}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import io.circe.literal.JsonStringContext
import io.renku.eventlog.DbEventLogGenerators._
import eu.timepit.refined.auto._
import io.renku.eventlog.{CompoundId, Event, EventLogDataFetching, EventLogDataProvisioning, InMemoryEventLogDb}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventStatusRenamerImplSpec
    extends AnyWordSpec
    with DbInitSpec
    with should.Matchers
    with EventLogDataProvisioning
    with EventLogDataFetching {
  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    latestEventDatesViewRemover,
    projectTableCreator,
    projectPathRemover,
    eventLogTableRenamer
  )

  "run" should {
    s"rename all the events from PROCESSING to GENERATING_TRIPLES " in new TestCase {
      val processingEvents = events.generateNonEmptyList(minElements = 2)
      processingEvents.map(event => store(event, withStatus = "PROCESSING"))
      val otherEvents = events.generateNonEmptyList()
      otherEvents.map(event => store(event, withStatus = event.status.toString))

      eventStatusRenamer.run().unsafeRunSync() shouldBe ((): Unit)

      findEvents(status = GeneratingTriples).eventIdsOnly.toSet shouldBe processingEvents
        .map(_.compoundEventId)
        .toList
        .toSet

      logger.loggedOnly(Info(s"'PROCESSING' event status renamed to 'GENERATING_TRIPLES'"))
    }

    s"Not to anything if there are no events with the status PROCESSING" in new TestCase {
      val otherEvents = events.generateNonEmptyList()
      otherEvents.map(event => store(event, withStatus = event.status.toString))

      eventStatusRenamer.run().unsafeRunSync() shouldBe ((): Unit)

      findEvents(status = GeneratingTriples).eventIdsOnly.toSet shouldBe Set.empty[CompoundId]

      findEventsId shouldBe otherEvents.map(_.id).toList.toSet

      logger.loggedOnly(Info(s"'PROCESSING' event status renamed to 'GENERATING_TRIPLES'"))
    }

  }

  private trait TestCase {
    val logger             = TestLogger[IO]()
    val eventStatusRenamer = new EventStatusRenamerImpl[IO](transactor, logger)
  }

  private def store(event: Event, withStatus: String): Unit = {
    upsertProject(event.compoundEventId, event.project.path, event.date)
    execute {
      sql"""|INSERT INTO 
            |event (event_id, project_id, status, created_date, execution_date, event_date, event_body, batch_date) 
            |values (
            |${event.id}, 
            |${event.project.id}, 
            |$withStatus, 
            |${createdDates.generateOne},
            |${executionDates.generateOne}, 
            |${event.date}, 
            |${toJsonBody(event)},
            |${event.batchDate})
      """.stripMargin.update.run.map(_ => ())
    }
  }

  private def toJsonBody(event: Event): String = json"""{
    "project": {
      "id": ${event.project.id.value},
      "path": ${event.project.path.value}
     }
  }""".noSpaces

  private def findEventsId: Set[EventId] =
    sql"SELECT event_id FROM event"
      .query[EventId]
      .to[List]
      .transact(transactor.get)
      .unsafeRunSync()
      .toSet
}
