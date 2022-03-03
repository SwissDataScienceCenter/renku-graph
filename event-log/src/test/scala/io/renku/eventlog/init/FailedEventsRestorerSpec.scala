/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.eventlog._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventId}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant.now

class FailedEventsRestorerSpec
    extends AnyWordSpec
    with IOSpec
    with DbInitSpec
    with should.Matchers
    with EventLogDataProvisioning
    with EventDataFetching {

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = allMigrations.takeWhile {
    case _: FailedEventsRestorerImpl[IO] => false
    case _ => true
  }

  "run" should {

    "change status of events in the given status matching the given message " +
      "if there are no newer events with successful statues for the project" in new TestCase {
        val eventDate = timestampsNotInTheFuture(butYoungerThan = now minusSeconds 60).generateAs(EventDate)
        val eventToChange = storeGeneratedEvent(GenerationNonRecoverableFailure,
                                                eventDate,
                                                projectId,
                                                projectPath,
                                                EventMessage(sentenceContaining(failure).generateOne).some
        )._1
        val olderMatchingEvent = storeGeneratedEvent(
          GenerationNonRecoverableFailure,
          timestamps(max = eventDate.value).generateAs(EventDate),
          projectId,
          projectPath,
          EventMessage(sentenceContaining(failure).generateOne).some
        )._1
        val newerMatchingEvent = storeGeneratedEvent(
          GenerationNonRecoverableFailure,
          timestampsNotInTheFuture(butYoungerThan = eventDate.value).generateAs(EventDate),
          projectId,
          projectPath,
          EventMessage(sentenceContaining(failure).generateOne).some
        )._1
        val olderNotMatchingEvent = storeGeneratedEvent(
          TriplesStore,
          timestamps(max = eventDate.value).generateAs(EventDate),
          projectId,
          projectPath
        )._1

        restorer.run().unsafeRunSync() shouldBe ()

        findEvent(compoundId(eventToChange)).map(_._2)         shouldBe New.some
        findEvent(compoundId(olderMatchingEvent)).map(_._2)    shouldBe New.some
        findEvent(compoundId(newerMatchingEvent)).map(_._2)    shouldBe New.some
        findEvent(compoundId(olderNotMatchingEvent)).map(_._2) shouldBe TriplesStore.some

        logger.loggedOnly(Info(s"3 events restored for processing from '$failure'"))
      }

    "do not change status of events " +
      "if there are newer events with successful statues" in new TestCase {
        val eventDate = timestampsNotInTheFuture(butYoungerThan = now minusSeconds 60).generateAs(EventDate)
        val matchingEvent = storeGeneratedEvent(GenerationNonRecoverableFailure,
                                                eventDate,
                                                projectId,
                                                projectPath,
                                                EventMessage(sentenceContaining(failure).generateOne).some
        )._1
        val olderMatchingEvent = storeGeneratedEvent(
          GenerationNonRecoverableFailure,
          timestamps(max = eventDate.value).generateAs(EventDate),
          projectId,
          projectPath,
          EventMessage(sentenceContaining(failure).generateOne).some
        )._1
        val newerInDiscardingStatusEvent = storeGeneratedEvent(
          TriplesStore,
          timestampsNotInTheFuture(butYoungerThan = eventDate.value).generateAs(EventDate),
          projectId,
          projectPath
        )._1

        restorer.run().unsafeRunSync() shouldBe ()

        findEvent(compoundId(matchingEvent)).map(_._2)                shouldBe GenerationNonRecoverableFailure.some
        findEvent(compoundId(olderMatchingEvent)).map(_._2)           shouldBe GenerationNonRecoverableFailure.some
        findEvent(compoundId(newerInDiscardingStatusEvent)).map(_._2) shouldBe TriplesStore.some

        logger.loggedOnly(Info(s"0 events restored for processing from '$failure'"))
      }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne
    val failure: NonBlank = Refined.unsafeApply(s"%${sentenceContaining("%").generateOne}%")

    def compoundId(eventId: EventId) = CompoundEventId(eventId, projectId)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val restorer = new FailedEventsRestorerImpl[IO](failure.value,
                                                    currentStatus = GenerationNonRecoverableFailure,
                                                    destinationStatus = New,
                                                    discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
    )
  }
}
