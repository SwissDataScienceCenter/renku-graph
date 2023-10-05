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

import cats.effect.Async
import io.circe.Json
import io.renku.db.syntax.{CommandDef, _}
import io.renku.db.{DbClient, SqlStatement}
import io.renku.events.CategoryName
import skunk._
import skunk.implicits._

import java.time.{OffsetDateTime, Duration => JDuration}
import scala.concurrent.duration._

private trait EventsRepository[F[_]] {
  def insert(category:             CategoryName, payload: Json): CommandDef[F]
  def fetchEvents(category:        CategoryName): QueryDef[F, List[DequeuedEvent]]
  def markUnderProcessing(eventId: Int): CommandDef[F]
  def returnToQueue(eventId:     Int): CommandDef[F]
  def delete(eventId:              Int): CommandDef[F]
}

private object EventsRepository {
  def apply[F[_]: Async, DB]: EventsRepository[F] =
    new EventsRepositoryImpl[F, DB]

  val gracePeriod: JDuration = JDuration.ofSeconds((30 minutes).toSeconds)
}

private class EventsRepositoryImpl[F[_]: Async, DB] extends DbClient[F](maybeHistogram = None) with EventsRepository[F] {

  import EventsRepository._
  import TypeSerializers._
  import skunk.codec.all.{int4, text, timestamptz}

  private val eventsChunkSize: Int = 50

  override def insert(category: CategoryName, payload: Json): CommandDef[F] = measureExecutionTime {
    val timestamp = OffsetDateTime.now()
    SqlStatement
      .named(s"$queryPrefix insert")
      .command[CategoryName *: String *: OffsetDateTime *: OffsetDateTime *: EnqueueStatus *: EmptyTuple](
        sql"""INSERT INTO enqueued_event (category, payload, created, updated, status)
              VALUES ($categoryNameEncoder, $text, $timestamptz, $timestamptz, $enqueueStatusEncoder)""".command
      )
      .arguments(category *: payload.noSpaces *: timestamp *: timestamp *: EnqueueStatus.New *: EmptyTuple)
      .build
      .void
  }

  override def fetchEvents(category: CategoryName): QueryDef[F, List[DequeuedEvent]] = measureExecutionTime {
    SqlStatement
      .named(s"$queryPrefix fetch")
      .select[CategoryName *: EnqueueStatus *: EnqueueStatus *: OffsetDateTime *: EmptyTuple, DequeuedEvent](
        fetchQuery
      )
      .arguments(
        category *: EnqueueStatus.New *: EnqueueStatus.Processing *:
          (OffsetDateTime.now() minus gracePeriod) *: EmptyTuple
      )
      .build(_.toList)
  }

  private lazy val fetchQuery
      : Query[CategoryName *: EnqueueStatus *: EnqueueStatus *: OffsetDateTime *: EmptyTuple, DequeuedEvent] =
    sql"""SELECT id, payload
          FROM enqueued_event
          WHERE category = $categoryNameEncoder
                AND ((status = $enqueueStatusEncoder)
                      OR (status = $enqueueStatusEncoder AND $timestamptz >= updated)
                    )
          LIMIT #${eventsChunkSize.toString}"""
      .query(int4 ~ text)
      .map { case (id: Int) ~ (payload: String) => DequeuedEvent(id, payload) }

  override def markUnderProcessing(eventId: Int): CommandDef[F] = measureExecutionTime {
    SqlStatement
      .named(s"$queryPrefix update to processing")
      .command[OffsetDateTime *: EnqueueStatus *: Int *: EmptyTuple](
        sql"""UPDATE enqueued_event
              SET updated = $timestamptz, status = $enqueueStatusEncoder
              WHERE id = $int4""".command
      )
      .arguments(OffsetDateTime.now() *: EnqueueStatus.Processing *: eventId *: EmptyTuple)
      .build
      .void
  }

  override def returnToQueue(eventId: Int): CommandDef[F] = measureExecutionTime {
    SqlStatement
      .named(s"$queryPrefix update to new")
      .command[OffsetDateTime *: EnqueueStatus *: Int *: EmptyTuple](
        sql"""UPDATE enqueued_event
              SET updated = $timestamptz, status = $enqueueStatusEncoder
              WHERE id = $int4""".command
      )
      .arguments(OffsetDateTime.now() *: EnqueueStatus.New *: eventId *: EmptyTuple)
      .build
      .void
  }

  override def delete(eventId: Int): CommandDef[F] = measureExecutionTime {
    SqlStatement
      .named(s"$queryPrefix delete")
      .command[Int *: EnqueueStatus *: EmptyTuple](
        sql"""DELETE FROM enqueued_event
              WHERE id = $int4 AND status = $enqueueStatusEncoder""".command
      )
      .arguments(eventId *: EnqueueStatus.Processing *: EmptyTuple)
      .build
      .void
  }

  private lazy val queryPrefix = "queue event -"
}
