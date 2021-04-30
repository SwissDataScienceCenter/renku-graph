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
import ch.datascience.interpreters.TestLogger
import io.renku.eventlog.InMemoryEventLogDb

trait EventLogDbMigrations {
  self: InMemoryEventLogDb =>

  private lazy val logger = TestLogger[IO]()

  protected lazy val eventLogTableCreator:     Migration = EventLogTableCreator(sessionResource, logger)
  protected lazy val projectPathAdder:         Migration = ProjectPathAdder(sessionResource, logger)
  protected lazy val batchDateAdder:           Migration = BatchDateAdder(sessionResource, logger)
  protected lazy val projectTableCreator:      Migration = ProjectTableCreator(sessionResource, logger)
  protected lazy val projectPathRemover:       Migration = ProjectPathRemover(sessionResource, logger)
  protected lazy val eventLogTableRenamer:     Migration = EventLogTableRenamer(sessionResource, logger)
  protected lazy val eventStatusRenamer:       Migration = EventStatusRenamer(sessionResource, logger)
  protected lazy val eventPayloadTableCreator: Migration = EventPayloadTableCreator(sessionResource, logger)
  protected lazy val eventPayloadSchemaVersionAdder: Migration =
    EventPayloadSchemaVersionAdder(sessionResource, logger)
  protected lazy val subscriptionCategorySyncTimeTableCreator: Migration =
    SubscriptionCategorySyncTimeTableCreator(sessionResource, logger)
  protected lazy val statusesProcessingTimeTableCreator: Migration =
    StatusesProcessingTimeTableCreator(sessionResource, logger)
  protected lazy val subscriberTableCreator:    Migration = SubscriberTableCreator(sessionResource, logger)
  protected lazy val eventDeliveryTableCreator: Migration = EventDeliveryTableCreator(sessionResource, logger)
  protected lazy val timestampZoneAdder:        Migration = TimestampZoneAdder(sessionResource, logger)

  protected type Migration = { def run(): IO[Unit] }

  protected lazy val allMigrations: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    projectTableCreator,
    projectPathRemover,
    eventLogTableRenamer,
    eventStatusRenamer,
    eventPayloadTableCreator,
    eventPayloadSchemaVersionAdder,
    subscriptionCategorySyncTimeTableCreator,
    statusesProcessingTimeTableCreator,
    subscriberTableCreator,
    eventDeliveryTableCreator,
    timestampZoneAdder
  )
}
