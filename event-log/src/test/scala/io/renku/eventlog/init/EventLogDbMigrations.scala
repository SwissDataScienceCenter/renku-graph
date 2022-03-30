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

  protected[init] lazy val eventLogTableCreator:           DbMigrator[IO] = EventLogTableCreator[IO]
  protected[init] lazy val projectPathAdder:               DbMigrator[IO] = ProjectPathAdder[IO]
  protected[init] lazy val batchDateAdder:                 DbMigrator[IO] = BatchDateAdder[IO]
  protected[init] lazy val projectTableCreator:            DbMigrator[IO] = ProjectTableCreator[IO]
  protected[init] lazy val projectPathRemover:             DbMigrator[IO] = ProjectPathRemover[IO]
  protected[init] lazy val eventLogTableRenamer:           DbMigrator[IO] = EventLogTableRenamer[IO]
  protected[init] lazy val eventStatusRenamer:             DbMigrator[IO] = EventStatusRenamer[IO]
  protected[init] lazy val eventPayloadTableCreator:       DbMigrator[IO] = EventPayloadTableCreator[IO]
  protected[init] lazy val eventPayloadSchemaVersionAdder: DbMigrator[IO] = EventPayloadSchemaVersionAdder[IO]
  protected[init] lazy val subscriptionCategorySyncTimeTableCreator: DbMigrator[IO] =
    SubscriptionCategorySyncTimeTableCreator[IO]
  protected[init] lazy val statusesProcessingTimeTableCreator: DbMigrator[IO] =
    StatusesProcessingTimeTableCreator[IO]
  protected[init] lazy val subscriberTableCreator:         DbMigrator[IO] = SubscriberTableCreator[IO]
  protected[init] lazy val eventDeliveryTableCreator:      DbMigrator[IO] = EventDeliveryTableCreator[IO]
  protected[init] lazy val timestampZoneAdder:             DbMigrator[IO] = TimestampZoneAdder[IO]
  protected[init] lazy val payloadTypeChanger:             DbMigrator[IO] = PayloadTypeChanger[IO]
  protected[init] lazy val statusChangeEventsTableCreator: DbMigrator[IO] = StatusChangeEventsTableCreator[IO]
  protected[init] lazy val eventDeliveryEventTypeAdder:    DbMigrator[IO] = EventDeliveryEventTypeAdder[IO]
  protected[init] lazy val tsMigrationTableCreator:        DbMigrator[IO] = TSMigrationTableCreator[IO]
  protected[init] lazy val cleanUpEventsTableCreator:      DbMigrator[IO] = CleanUpEventsTableCreator[IO]

  protected[init] lazy val allMigrations: List[DbMigrator[IO]] = List(
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
    eventDeliveryEventTypeAdder,
    tsMigrationTableCreator,
    cleanUpEventsTableCreator
  )
}
