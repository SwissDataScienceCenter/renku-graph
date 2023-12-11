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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.relativeTimestamps
import io.renku.graph.model.EventContentGenerators.{eventDates, executionDates}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventStatuses, processingStatuses}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.Duration

class LostZombieEventFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {
  private val projectSlug = projectSlugs.generateOne

  it should "do nothing if there are no zombie events in the table" in testDBResource.use { implicit cfg =>
    addRandomEvent() >>
      finder.popEvent().asserting(_ shouldBe None)
  }

  it should "do nothing if there are zombie events in the table but all were added less than 5 minutes ago" in testDBResource
    .use { implicit cfg =>
      addRandomEvent() >>
        addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne) >>
        addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne) >>
        finder.popEvent().asserting(_ shouldBe None)
    }

  it should "return a zombie event which has not been picked up" in testDBResource.use { implicit cfg =>
    for {
      _ <- addRandomEvent()
      _ <- addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne)

      zombieEventId     = compoundEventIds.generateOne
      zombieEventStatus = processingStatuses.generateOne
      _ <- addZombieEvent(zombieEventId, lostZombieEventExecutionDate.generateOne, zombieEventStatus)

      _ <- finder
             .popEvent()
             .asserting(_ shouldBe ZombieEvent(finder.processName, zombieEventId, projectSlug, zombieEventStatus).some)

      _ <- finder.popEvent().asserting(_ shouldBe None)
    } yield Succeeded
  }

  it should "return None if an event is in the past and the status is GeneratingTriples or TransformingTriples " +
    "but the message is not a zombie message" in testDBResource.use { implicit cfg =>
      addRandomEvent(lostZombieEventExecutionDate.generateOne, processingStatuses.generateOne) >>
        addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne) >>
        finder.popEvent().asserting(_ shouldBe None)
    }

  private val executionDateThreshold = 5 * 60
  private lazy val activeZombieEventExecutionDate =
    relativeTimestamps(lessThanAgo = Duration.ofSeconds(executionDateThreshold - 2)).toGeneratorOf(ExecutionDate)
  private lazy val lostZombieEventExecutionDate =
    relativeTimestamps(moreThanAgo = Duration.ofSeconds(executionDateThreshold + 2)).toGeneratorOf(ExecutionDate)

  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new LostZombieEventFinder[IO]
  }

  private def addRandomEvent(executionDate: ExecutionDate = executionDates.generateOne,
                             status:        EventStatus = eventStatuses.generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    storeEvent(compoundEventIds.generateOne, status, executionDate, eventDates.generateOne, eventBodies.generateOne)

  private def addZombieEvent(eventId:       CompoundEventId,
                             executionDate: ExecutionDate,
                             status:        EventStatus = processingStatuses.generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    storeEvent(eventId,
               status,
               executionDate,
               eventDates.generateOne,
               eventBodies.generateOne,
               projectSlug = projectSlug,
               maybeMessage = Some(EventMessage(zombieMessage))
    )
}
