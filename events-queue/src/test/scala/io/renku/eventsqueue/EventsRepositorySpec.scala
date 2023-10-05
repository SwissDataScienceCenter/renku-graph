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

package io.renku.eventsqueue

import Generators.events
import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import io.renku.db.syntax.{CommandDef, QueryDef}
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.eventsqueue.TypeSerializers.{categoryNameEncoder, enqueueStatusEncoder}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.javaDurations
import io.renku.testtools.CustomAsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.codec.all.{int4, text, timestamptz}
import skunk.implicits._

import java.time.{OffsetDateTime, Duration => JDuration}

class EventsRepositorySpec extends AsyncFlatSpec with CustomAsyncIOSpec with EventsQueueDBSpec with should.Matchers {

  private val category = categoryNames.generateOne

  it should "insert rows in status New into the DB so they are ready for processing" in {

    val allEvents = events.generateList().map(_.asJson)

    withDB.surround {
      for {
        _   <- allEvents.traverse_(e => execute(repo.insert(category, e))).assertNoException
        res <- execute(repo.fetchEvents(category)).asserting(_.map(_.payload) shouldBe allEvents.map(_.noSpaces))
      } yield res
    }
  }

  it should "update rows in status New so they are not visible for processing" in {

    val allEvents = events.generateList(min = 2).map(_.asJson)

    withDB.surround {
      for {
        _           <- allEvents.traverse_(e => execute(repo.insert(category, e))).assertNoException
        allDbEvents <- findAllEvents()
        updatedEvents = allDbEvents.zipWithIndex.collect { case (e, idx) if idx % 2 == 0 => e }
        _ <- updatedEvents.traverse_(e => execute(repo.markUnderProcessing(e.id))).assertNoException
        res <- execute(repo.fetchEvents(category)).asserting(
                 _.map(_.payload) shouldBe (allEvents.map(_.noSpaces) diff updatedEvents.map(_.payload))
               )
      } yield res
    }
  }

  it should "only delete rows in status PROCESSING" in {

    val newEvents        = events.generateList(min = 2).map(_.asJson)
    val processingEvents = events.generateList(min = 2).map(_.asJson)
    val allEvents        = newEvents ::: processingEvents

    withDB.surround {
      for {
        _ <- allEvents.traverse_(e => execute(repo.insert(category, e))).assertNoException
        _ <- processingEvents.traverse_ { event =>
               update(event.asJson, EnqueueStatus.Processing, OffsetDateTime.now())
             }
        allDbEvents <- findAllEvents()
        _           <- allDbEvents.traverse_(e => execute(repo.delete(e.id))).assertNoException
        res         <- findAllEvents().asserting(_.map(_.payload) shouldBe newEvents.map(_.noSpaces))
      } yield res
    }
  }

  it should "not pick events for processing " +
    "if they are in the Processing status for less than the grace period" in {

      val moreThanGracePeriod = events.generateOne.asJson
      val inGracePeriod       = events.generateOne.asJson
      val lessThanGracePeriod = events.generateOne.asJson
      val allEvents           = List(moreThanGracePeriod, inGracePeriod, lessThanGracePeriod)

      withDB.surround {
        for {
          _ <- allEvents.traverse_(e => execute(repo.insert(category, e))).assertNoException
          _ <- update(
                 moreThanGracePeriod,
                 EnqueueStatus.Processing,
                 gracePeriodFromNow.plus(javaDurations(min = JDuration.ofSeconds(1)).generateOne)
               )
          _ <- update(inGracePeriod, EnqueueStatus.Processing, gracePeriodFromNow)
          _ <- update(
                 lessThanGracePeriod,
                 EnqueueStatus.Processing,
                 gracePeriodFromNow.minus(javaDurations(min = JDuration.ofSeconds(1)).generateOne)
               )
          res <- execute(repo.fetchEvents(category)).asserting(
                   _.map(_.payload) shouldBe List(inGracePeriod.noSpaces, lessThanGracePeriod.noSpaces)
                 )
        } yield res
      }
    }

  it should "be possible to change status of an event in Processing back to New" in {

    val event = events.generateOne.asJson

    withDB.surround {
      for {
        _       <- execute(repo.insert(category, event))
        _       <- execute(repo.fetchEvents(category)).asserting(_.map(_.payload) shouldBe List(event.noSpaces))
        dbEvent <- findAllEvents().map(_.headOption.getOrElse(fail("Event expected")))
        _       <- execute(repo.markUnderProcessing(dbEvent.id))
        _       <- execute(repo.fetchEvents(category)).asserting(_ shouldBe Nil)
        _       <- execute(repo.returnToQueue(dbEvent.id))
        res     <- execute(repo.fetchEvents(category)).asserting(_.map(_.payload) shouldBe List(event.noSpaces))
      } yield res
    }
  }

  private def findAllEvents(): IO[List[DequeuedEvent]] = {
    val query: Query[CategoryName, DequeuedEvent] =
      sql"""SELECT id, payload FROM enqueued_event WHERE category = $categoryNameEncoder"""
        .query(int4 ~ text)
        .map { case (id: Int, payload: String) => DequeuedEvent(id, payload) }

    execute {
      QueryDef[IO, List[DequeuedEvent]] {
        _.prepare(query).flatMap(_.stream(category, 10).compile.toList)
      }
    }
  }

  private def update(event: Json, status: EnqueueStatus, date: OffsetDateTime) =
    findInDB(event)
      .flatMap(e => updateEvent(e.id, status, date))

  private def findInDB(event: Json) =
    findAllEvents()
      .map(_.find(_.payload == event.noSpaces).getOrElse(fail(s"no event with '${event.noSpaces}' payload")))

  private def updateEvent(id: Int, status: EnqueueStatus, updateDate: OffsetDateTime): IO[Unit] = {
    val query: Command[EnqueueStatus *: OffsetDateTime *: Int *: EmptyTuple] =
      sql"""UPDATE enqueued_event
            SET status = $enqueueStatusEncoder, updated = $timestamptz
            WHERE id = $int4
            """.command

    execute {
      CommandDef[IO] {
        _.prepare(query).flatMap(_.execute(status, updateDate, id)).void
      }
    }
  }

  private lazy val gracePeriodFromNow = OffsetDateTime.now().minus(EventsRepository.gracePeriod)

  private lazy val repo = new EventsRepositoryImpl[IO, TestDB]
}
