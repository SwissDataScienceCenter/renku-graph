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

package io.renku.entities.viewings.collector.projects.viewed

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.{Chunk, Stream}
import io.circe.syntax._
import io.renku.eventsqueue.Generators.dequeuedEvents
import io.renku.eventsqueue.{DequeuedEvent, EventsQueue}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.api.events.Generators.{projectViewedEvents, userIds}
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EventProcessorSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "define events chunk transformation that " +
    "decodes the each event's payload " +
    "groups by slug when there's no userId " +
    "and persists the item with the most recent date for each group" in {

      val slug1       = projectSlugs.generateOne
      val event1Slug1 = ProjectViewedEvent.forProject(slug1)
      val event2Slug1 = event1Slug1.copy(dateViewed =
        timestampsNotInTheFuture(butYoungerThan = event1Slug1.dateViewed.value).generateAs(projects.DateViewed)
      )
      val slug2       = projectSlugs.generateOne
      val event1Slug2 = ProjectViewedEvent.forProject(slug2)
      val event2Slug2 =
        event1Slug2.copy(dateViewed =
          timestamps(max = event1Slug2.dateViewed.value.minusSeconds(2)).generateAs(projects.DateViewed)
        )

      val events = List(
        dequeuedEvents.map(_.copy(payload = event1Slug1.asJson.noSpaces)).generateOne,
        dequeuedEvents.map(_.copy(payload = event2Slug1.asJson.noSpaces)).generateOne,
        dequeuedEvents.map(_.copy(payload = event1Slug2.asJson.noSpaces)).generateOne,
        dequeuedEvents.map(_.copy(payload = event2Slug2.asJson.noSpaces)).generateOne
      )

      givenPersisting(event2Slug1, returning = ().pure[IO])
      givenPersisting(event1Slug2, returning = ().pure[IO])

      Stream.emit[IO, Chunk[DequeuedEvent]](Chunk.from(events)).through(processor).compile.drain.assertNoException
    }

  it should "define a event transformation that " +
    "decodes the event payload " +
    "groups by slug and userId and " +
    "persists the item with the most recent date for each group" in {

      val slug       = projectSlugs.generateOne
      val event1Slug = ProjectViewedEvent.forProject(slug)
      val event2Slug = event1Slug
        .copy(
          dateViewed =
            timestampsNotInTheFuture(butYoungerThan = event1Slug.dateViewed.value).generateAs(projects.DateViewed),
          maybeUserId = userIds.generateSome
        )

      val events = List(
        dequeuedEvents.map(_.copy(payload = event1Slug.asJson.noSpaces)).generateOne,
        dequeuedEvents.map(_.copy(payload = event2Slug.asJson.noSpaces)).generateOne
      )

      givenPersisting(event1Slug, returning = ().pure[IO])
      givenPersisting(event2Slug, returning = ().pure[IO])

      Stream.emit[IO, Chunk[DequeuedEvent]](Chunk.from(events)).through(processor).compile.drain.assertNoException
    }

  it should "handle parsing/decoding errors by logging them as errors" in {

    val slug       = projectSlugs.generateOne
    val event1Slug = ProjectViewedEvent.forProject(slug)
    val event3Slug = event1Slug
      .copy(dateViewed =
        timestampsNotInTheFuture(butYoungerThan = event1Slug.dateViewed.value).generateAs(projects.DateViewed)
      )

    val events = List(
      dequeuedEvents.map(_.copy(payload = event1Slug.asJson.noSpaces)).generateOne,
      dequeuedEvents.map(_.copy(payload = "")).generateOne,
      dequeuedEvents.map(_.copy(payload = event3Slug.asJson.noSpaces)).generateOne
    )

    givenPersisting(event3Slug, returning = ().pure[IO])

    Stream.emit[IO, Chunk[DequeuedEvent]](Chunk.from(events)).through(processor).compile.drain.assertNoException
  }

  it should "handle persisting errors by logging them as errors " +
    "and returning them event back to the queue" in {

      val eventSlug1 = projectViewedEvents.generateOne
      val deSlug1    = dequeuedEvents.map(_.copy(payload = eventSlug1.asJson.noSpaces)).generateOne
      val eventSlug2 = projectViewedEvents.generateOne
      val deSlug2    = dequeuedEvents.map(_.copy(payload = eventSlug2.asJson.noSpaces)).generateOne
      val eventSlug3 = projectViewedEvents.generateOne
      val deSlug3    = dequeuedEvents.map(_.copy(payload = eventSlug3.asJson.noSpaces)).generateOne

      givenPersisting(eventSlug1, returning = ().pure[IO])
      val exception = exceptions.generateOne
      givenPersisting(eventSlug2, returning = exception.raiseError[IO, Unit])
      givenPersisting(eventSlug3, returning = ().pure[IO])

      givenReturningEventToQueue(deSlug2, returning = ().pure[IO])

      Stream[IO, Chunk[DequeuedEvent]](Chunk(deSlug1, deSlug2, deSlug3))
        .through(processor)
        .compile
        .drain
        .assertNoException
    }

  private implicit val logger: TestLogger[IO] = TestLogger()
  private val persister      = mock[EventPersister[IO]]
  private val eventsQueue    = mock[EventsQueue[IO]]
  private lazy val processor = new EventProcessorImpl[IO](persister, eventsQueue)

  private def givenPersisting(event: ProjectViewedEvent, returning: IO[Unit]) =
    (persister.persist _)
      .expects(event)
      .returning(returning)

  private def givenReturningEventToQueue(event: DequeuedEvent, returning: IO[Unit]) =
    (eventsQueue.returnToQueue _)
      .expects(event)
      .returning(returning)
}
