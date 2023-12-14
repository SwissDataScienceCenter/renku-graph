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

package io.renku.eventlog.events.producers.zombieevents

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{EventStatus, ExecutionDate}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.Duration

class LongProcessingEventFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  it should "return an event " +
    s"if it's in one of the 'running' statuses for more than the relevant grace period " +
    "and there's info about its delivery" in testDBResource.use { implicit cfg =>
      List(
        GeneratingTriples   -> Duration.ofDays(6 * 7),
        TransformingTriples -> Duration.ofDays(1),
        Deleting            -> Duration.ofDays(1)
      ).traverse_ { case (status, gracePeriod) =>
        `return an event in the presence of delivery info`(status, gracePeriod)
      }
    }

  private def `return an event in the presence of delivery info`(status: ProcessingStatus, gracePeriod: Duration)(
      implicit cfg: DBConfig[EventLogDB]
  ) = {
    val project = consumerProjects.generateOne
    for {
      oldEnoughEvent <- addEvent(
                          status,
                          relativeTimestamps(moreThanAgo = gracePeriod plusHours 1).generateAs(ExecutionDate),
                          project
                        )
      _ <- upsertEventDeliveryInfo(oldEnoughEvent.eventId)

      tooYoungEvent <- addEvent(
                         status,
                         relativeTimestamps(lessThanAgo = gracePeriod minusHours 1).generateAs(ExecutionDate),
                         project
                       )
      _ <- upsertEventDeliveryInfo(tooYoungEvent.eventId)

      _ <- finder
             .popEvent()
             .asserting(_ shouldBe Some(ZombieEvent(finder.processName, oldEnoughEvent.eventId, project.slug, status)))
      res <- finder.popEvent().asserting(_ shouldBe None)
    } yield res
  }

  it should "return an event " +
    "if it's in one of the 'running' statuses " +
    "there's no info about its delivery " +
    "and it's in status for more than 10 minutes" in testDBResource.use { implicit cfg =>
      List(GeneratingTriples, TransformingTriples, Deleting).traverse_ { status =>
        val project = consumerProjects.generateOne
        for {
          event <- addEvent(
                     status,
                     relativeTimestamps(moreThanAgo = Duration ofMinutes 11).generateAs(ExecutionDate),
                     project
                   )
          _ <- addEvent(
                 status,
                 relativeTimestamps(lessThanAgo = Duration ofMinutes 9).generateAs(ExecutionDate),
                 project
               )
          _ <- finder
                 .popEvent()
                 .asserting(_ shouldBe Some(ZombieEvent(finder.processName, event.eventId, project.slug, status)))
          res <- finder.popEvent().asserting(_ shouldBe None)
        } yield res
      }
    }

  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new LongProcessingEventFinder[IO]
  }

  private def addEvent(status: EventStatus, executionDate: ExecutionDate, project: Project)(implicit
      cfg: DBConfig[EventLogDB]
  ) = storeGeneratedEvent(status, eventDates.generateOne, project, executionDate)
}
