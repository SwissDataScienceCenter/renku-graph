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

package io.renku.eventlog.events.producers.tsmigrationrequest

import cats.effect.IO
import io.renku.config.ServiceVersion
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.TSMigtationTypeSerializers._
import io.renku.eventlog._
import io.renku.events.Subscription.SubscriberUrl
import skunk._
import skunk.implicits._

trait TsMigrationTableProvisioning {
  self: EventLogPostgresSpec with TypeSerializers =>

  protected def insertSubscriptionRecord(url:        SubscriberUrl,
                                         version:    ServiceVersion,
                                         status:     MigrationStatus,
                                         changeDate: ChangeDate
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[ServiceVersion *: SubscriberUrl *: MigrationStatus *: ChangeDate *: EmptyTuple] = sql"""
        INSERT INTO ts_migration (subscriber_version, subscriber_url, status, change_date)
        VALUES ($serviceVersionEncoder, $subscriberUrlEncoder, $migrationStatusEncoder, $changeDateEncoder)
        """.command
      session
        .prepare(query)
        .flatMap(_.execute(version *: url *: status *: changeDate *: EmptyTuple))
        .void
    }

  protected def findRow(url: SubscriberUrl, version: ServiceVersion)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[(MigrationStatus, ChangeDate)] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[SubscriberUrl *: ServiceVersion *: EmptyTuple, (MigrationStatus, ChangeDate)] = sql"""
          SELECT status, change_date
          FROM ts_migration
          WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder"""
        .query(migrationStatusDecoder ~ changeDateDecoder)
        .map { case status ~ changeDate => status -> changeDate }
      session.prepare(query).flatMap(_.unique(url *: version *: EmptyTuple))
    }

  protected def findRows(
      version: ServiceVersion
  )(implicit cfg: DBConfig[EventLogDB]): IO[Set[(SubscriberUrl, MigrationStatus, ChangeDate)]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[ServiceVersion, (SubscriberUrl, MigrationStatus, ChangeDate)] = sql"""
          SELECT subscriber_url, status, change_date
          FROM ts_migration
          WHERE subscriber_version = $serviceVersionEncoder"""
        .query(subscriberUrlDecoder ~ migrationStatusDecoder ~ changeDateDecoder)
        .map { case url ~ status ~ changeDate => (url, status, changeDate) }
      session.prepare(query).flatMap(_.stream(version, 32).compile.toList.map(_.toSet))
    }

  protected def findMessage(url: SubscriberUrl, version: ServiceVersion)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Option[MigrationMessage]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[SubscriberUrl *: ServiceVersion *: EmptyTuple, Option[MigrationMessage]] = sql"""
        SELECT message
        FROM ts_migration
        WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder"""
        .query(migrationMessageDecoder.opt)
      session.prepare(query).flatMap(_.unique(url *: version *: EmptyTuple))
    }
}
