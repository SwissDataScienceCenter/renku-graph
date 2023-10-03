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

import DBInfra.QueueTable
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.interpreters.TestLogger
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{Outcome, Succeeded}
import skunk._
import skunk.codec.all.{bool, varchar}
import skunk.implicits._

class EventsQueueDBCreatorSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with ContainerDB {

  it should "create an 'enqueued-event' table if not exists" in {
    for {
      _ <- checkTableExists(QueueTable.name).asserting(_ shouldBe false)

      _ <- execute(dbInfraCreator.createDBInfra).assertNoException

      _ <- checkTableExists(QueueTable.name).asserting(_ shouldBe true)
      _ <- verifyIndexExists(QueueTable.name, "idx_enqueued_event_category").assertNoException
      _ <- verifyIndexExists(QueueTable.name, "idx_enqueued_event_payload").assertNoException
      _ <- verifyIndexExists(QueueTable.name, "idx_enqueued_event_created").assertNoException
      _ <- verifyIndexExists(QueueTable.name, "idx_enqueued_event_updated").assertNoException
      _ <- verifyIndexExists(QueueTable.name, "idx_enqueued_event_status").assertNoException
    } yield Succeeded
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private lazy val dbInfraCreator = new EventsQueueDBCreatorImpl[IO]

  private def checkTableExists(table: String): IO[Boolean] = execute[Boolean] {
    val query: Query[String, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $varchar)".query(bool)
    Kleisli(_.prepare(query).flatMap(_.unique(table)).recover { case _ => false })
  }

  def verifyIndexExists(table: String, index: String) = execute[Outcome] {
    val query: Query[String *: String *: EmptyTuple, Boolean] =
      sql"""SELECT EXISTS (
              SELECT 1
              FROM pg_indexes
              WHERE tablename = $varchar AND indexname = $varchar
            )""".query(bool)
    Kleisli {
      _.prepare(query)
        .flatMap(_.unique(table *: index *: EmptyTuple))
        .recover { case _ => false }
        .map {
          case true  => Succeeded
          case false => fail(s"'$index' index on '$table' does not exist")
        }
    }
  }
}
