/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.{EventLogDB, EventLogDBProvisioning}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.Instant.now

class FailedEventsRestorerSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with DbInitSpec
    with should.Matchers
    with EventLogDBProvisioning {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[FailedEventsRestorer[IO]]

  it should "change status of events in the given status matching the given message " +
    "if there are no newer events with successful statues for the project" in testDBResource.use { implicit cfg =>
      val eventDate = timestampsNotInTheFuture(butYoungerThan = now minusSeconds 60).generateAs(EventDate)
      for {
        eventToChange <- storeGeneratedEvent(GenerationNonRecoverableFailure,
                                             eventDate,
                                             project,
                                             message = sentenceContaining(failure).generateAs(EventMessage).some
                         )
        olderMatchingEvent <- storeGeneratedEvent(
                                GenerationNonRecoverableFailure,
                                timestamps(max = eventDate.value).generateAs(EventDate),
                                project,
                                message = sentenceContaining(failure).generateAs(EventMessage).some
                              )
        newerMatchingEvent <- storeGeneratedEvent(
                                GenerationNonRecoverableFailure,
                                timestampsNotInTheFuture(butYoungerThan = eventDate.value).generateAs(EventDate),
                                project,
                                message = sentenceContaining(failure).generateAs(EventMessage).some
                              )
        olderNotMatchingEvent <- storeGeneratedEvent(
                                   TriplesStore,
                                   timestamps(max = eventDate.value).generateAs(EventDate),
                                   project
                                 )

        _ <- restorer.run.assertNoException

        _ <- findEvent(eventToChange.eventId).asserting(_.map(_.status) shouldBe New.some)
        _ <- findEvent(olderMatchingEvent.eventId).asserting(_.map(_.status) shouldBe New.some)
        _ <- findEvent(newerMatchingEvent.eventId).asserting(_.map(_.status) shouldBe New.some)
        _ <- findEvent(olderNotMatchingEvent.eventId).asserting(_.map(_.status) shouldBe TriplesStore.some)

        _ <- logger.loggedOnlyF(Info(s"3 events restored for processing from '$failure'"))
      } yield Succeeded
    }

  it should "do not change status of events " +
    "if there are newer events with successful statues" in testDBResource.use { implicit cfg =>
      val eventDate = timestampsNotInTheFuture(butYoungerThan = now minusSeconds 60).generateAs(EventDate)
      for {
        matchingEvent <- storeGeneratedEvent(GenerationNonRecoverableFailure,
                                             eventDate,
                                             project,
                                             message = EventMessage(sentenceContaining(failure).generateOne).some
                         )
        olderMatchingEvent <- storeGeneratedEvent(
                                GenerationNonRecoverableFailure,
                                timestamps(max = eventDate.value).generateAs(EventDate),
                                project,
                                message = EventMessage(sentenceContaining(failure).generateOne).some
                              )
        newerInDiscardingStatusEvent <-
          storeGeneratedEvent(
            TriplesStore,
            timestampsNotInTheFuture(butYoungerThan = eventDate.value).generateAs(EventDate),
            project
          )

        _ <- restorer.run.assertNoException

        _ <- findEvent(matchingEvent.eventId).asserting(_.map(_.status) shouldBe GenerationNonRecoverableFailure.some)
        _ <- findEvent(olderMatchingEvent.eventId)
               .asserting(_.map(_.status) shouldBe GenerationNonRecoverableFailure.some)
        _ <- findEvent(newerInDiscardingStatusEvent.eventId).asserting(_.map(_.status) shouldBe TriplesStore.some)

        _ <- logger.loggedOnlyF(Info(s"0 events restored for processing from '$failure'"))
      } yield Succeeded
    }

  private lazy val project = consumerProjects.generateOne
  private lazy val failure: NonBlank = Refined.unsafeApply(s"%${sentenceContaining("%").generateOne}%")

  private def restorer(implicit cfg: DBConfig[EventLogDB]) =
    new FailedEventsRestorerImpl[IO](
      failure.value,
      currentStatus = GenerationNonRecoverableFailure,
      destinationStatus = New,
      discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
    )
}
