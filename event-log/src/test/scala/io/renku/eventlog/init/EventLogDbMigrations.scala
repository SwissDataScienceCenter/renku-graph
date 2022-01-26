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
import io.renku.eventlog.InMemoryEventLogDb
import io.renku.interpreters.TestLogger

trait EventLogDbMigrations {
  self: InMemoryEventLogDb =>

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()

  protected type Migration = { def run(): IO[Unit] }

  protected lazy val eventLogTableCreator:     Migration = EventLogTableCreator(sessionResource)
  protected lazy val projectPathAdder:         Migration = ProjectPathAdder(sessionResource)
  protected lazy val batchDateAdder:           Migration = BatchDateAdder(sessionResource)
  protected lazy val projectTableCreator:      Migration = ProjectTableCreator(sessionResource)
  protected lazy val projectPathRemover:       Migration = ProjectPathRemover(sessionResource)
  protected lazy val eventLogTableRenamer:     Migration = EventLogTableRenamer(sessionResource)
  protected lazy val eventStatusRenamer:       Migration = EventStatusRenamer(sessionResource)
  protected lazy val eventPayloadTableCreator: Migration = EventPayloadTableCreator(sessionResource)
  protected lazy val eventPayloadSchemaVersionAdder: Migration =
    EventPayloadSchemaVersionAdder(sessionResource)
  protected lazy val subscriptionCategorySyncTimeTableCreator: Migration =
    SubscriptionCategorySyncTimeTableCreator(sessionResource)
  protected lazy val statusesProcessingTimeTableCreator: Migration =
    StatusesProcessingTimeTableCreator(sessionResource)
  protected lazy val subscriberTableCreator:         Migration = SubscriberTableCreator(sessionResource)
  protected lazy val eventDeliveryTableCreator:      Migration = EventDeliveryTableCreator(sessionResource)
  protected lazy val timestampZoneAdder:             Migration = TimestampZoneAdder(sessionResource)
  protected lazy val payloadTypeChanger:             Migration = PayloadTypeChanger(sessionResource)
  protected lazy val statusChangeEventsTableCreator: Migration = StatusChangeEventsTableCreator(sessionResource)

  protected lazy val eventDeliveryEventTypeAdder: Migration = EventDeliveryEventTypeAdder(sessionResource)

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
    timestampZoneAdder,
    payloadTypeChanger,
    statusChangeEventsTableCreator,
    eventDeliveryEventTypeAdder
  )
}
