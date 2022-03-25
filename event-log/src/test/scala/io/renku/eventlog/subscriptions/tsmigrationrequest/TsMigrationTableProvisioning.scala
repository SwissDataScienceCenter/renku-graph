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

package io.renku.eventlog.subscriptions.tsmigrationrequest

import cats.data.Kleisli
import io.renku.eventlog.InMemoryEventLogDb
import io.renku.eventlog.subscriptions.tsmigrationrequest.TypeSerializers._
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.http.server.version.ServiceVersion
import skunk.implicits._
import skunk.{Command, Query, ~}

trait TsMigrationTableProvisioning {
  self: InMemoryEventLogDb =>

  private[tsmigrationrequest] def insertSubscriptionRecord(url:        SubscriberUrl,
                                                           version:    ServiceVersion,
                                                           status:     MigrationStatus,
                                                           changeDate: ChangeDate
  ): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[ServiceVersion ~ SubscriberUrl ~ MigrationStatus ~ ChangeDate] = sql"""
        INSERT INTO ts_migration (subscriber_version, subscriber_url, status, change_date)
        VALUES ($serviceVersionEncoder, $subscriberUrlEncoder, $migrationStatusEncoder, $changeDateEncoder)
        """.command
      session
        .prepare(query)
        .use(_.execute(version ~ url ~ status ~ changeDate))
        .void
    }
  }

  private[tsmigrationrequest] def findRows(url: SubscriberUrl, version: ServiceVersion): (MigrationStatus, ChangeDate) =
    execute {
      Kleisli { session =>
        val query: Query[SubscriberUrl ~ ServiceVersion, (MigrationStatus, ChangeDate)] = sql"""
            SELECT status, change_date
            FROM ts_migration
            WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder"""
          .query(migrationStatusDecoder ~ changeDateDecoder)
          .map { case status ~ changeDate => status -> changeDate }
        session.prepare(query).use(_.unique(url ~ version))
      }
    }

  private[tsmigrationrequest] def findMessage(url: SubscriberUrl, version: ServiceVersion): Option[MigrationMessage] =
    execute {
      Kleisli { session =>
        val query: Query[SubscriberUrl ~ ServiceVersion, MigrationMessage] = sql"""
            SELECT message
            FROM ts_migration
            WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder"""
          .query(migrationMessageDecoder)
        session.prepare(query).use(_.option(url ~ version))
      }
    }
}
