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

package io.renku.eventlog.init

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.interpreters.TestLogger
import io.renku.eventlog.InMemoryEventLogDb
import org.scalatest.{BeforeAndAfter, Suite}

import scala.language.reflectiveCalls

trait DbInitSpec extends InMemoryEventLogDb with BeforeAndAfter {
  self: Suite =>

  private val tablesToDropBeforeEachTest =
    List("event_payload", "status_processing_time", "event_log", "event", "subscription_category_sync_time", "project")

  private val logger = TestLogger[IO]()

  protected lazy val eventLogTableCreator:           Migration = EventLogTableCreator(transactor, logger)
  protected lazy val projectPathAdder:               Migration = ProjectPathAdder(transactor, logger)
  protected lazy val batchDateAdder:                 Migration = BatchDateAdder(transactor, logger)
  protected lazy val latestEventDatesViewRemover:    Migration = LatestEventDatesViewRemover(transactor, logger)
  protected lazy val projectTableCreator:            Migration = ProjectTableCreator(transactor, logger)
  protected lazy val projectPathRemover:             Migration = ProjectPathRemover(transactor, logger)
  protected lazy val eventLogTableRenamer:           Migration = EventLogTableRenamer(transactor, logger)
  protected lazy val eventStatusRenamer:             Migration = EventStatusRenamer(transactor, logger)
  protected lazy val eventPayloadTableCreator:       Migration = EventPayloadTableCreator(transactor, logger)
  protected lazy val eventPayloadSchemaVersionAdder: Migration = EventPayloadSchemaVersionAdder(transactor, logger)
  protected lazy val subscriptionCategorySyncTimeTableCreator: Migration =
    SubscriptionCategorySyncTimeTableCreator(transactor, logger)
  protected lazy val statusProcessingTimeTableCreator: Migration =
    StatusesProcessingTimeTableCreator(transactor, logger)

  protected type Migration = { def run(): IO[Unit] }

  protected val migrationsToRun: List[Migration]

  before {
    tablesToDropBeforeEachTest foreach dropTable
    migrationsToRun.map(_.run()).sequence.unsafeRunSync()
  }

  protected def createEventTable(): Unit =
    List(eventLogTableCreator, batchDateAdder, eventLogTableRenamer)
      .map(_.run())
      .sequence
      .void
      .unsafeRunSync()

}
