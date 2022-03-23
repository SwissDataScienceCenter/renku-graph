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

package io.renku.eventlog.subscriptions
package tsmigration

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions.tsmigration.MigrationStatus.Done
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.http.server.version.ServiceVersion
import io.renku.metrics.LabeledHistogram
import skunk.data.Completion
import skunk.implicits._
import skunk.{Session, SqlState, ~}

import java.time.Instant

private class MigrationSubscriberTracker[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F],
                                                                                  now: () => Instant = () =>
                                                                                    Instant.now()
) extends DbClient(Some(queriesExecTimes))
    with SubscriberTracker[F, MigratorSubscriptionInfo]
    with TypeSerializers {

  override def add(info: MigratorSubscriptionInfo): F[Boolean] = SessionResource[F].useK {
    migrationDone(info) flatMap {
      case false => insert(info)
      case true  => Kleisli.pure[F, Session[F], Boolean](false)
    } recoverWith falseForForeignKeyViolation
  }

  private def migrationDone(info: MigratorSubscriptionInfo): Kleisli[F, Session[F], Boolean] = measureExecutionTime {
    SqlStatement(name = "migrator - check")
      .select[ServiceVersion, SubscriberUrl](sql"""
        SELECT subscriber_url
        FROM ts_migration
        WHERE subscriber_version = $serviceVersionEncoder
          AND status = '#${Done.value}'
        LIMIT 1
        """.query(subscriberUrlDecoder))
      .arguments(info.subscriberVersion)
      .build(_.option)
      .mapResult(_.nonEmpty)
  }

  private def insert(info: MigratorSubscriptionInfo): Kleisli[F, Session[F], Boolean] = measureExecutionTime {
    SqlStatement(name = "migrator - add")
      .command[ServiceVersion ~ SubscriberUrl ~ MigrationStatus ~ ChangeDate](sql"""
        INSERT INTO ts_migration (subscriber_version, subscriber_url, status, change_date)
        VALUES ($serviceVersionEncoder, $subscriberUrlEncoder, $migrationStatusEncoder, $changeDateEncoder)
        ON CONFLICT (subscriber_version, subscriber_url)
        DO NOTHING
        """.command)
      .arguments(info.subscriberVersion ~ info.subscriberUrl ~ MigrationStatus.New ~ ChangeDate(now()))
      .build
  } map insertToTableResult

  private lazy val insertToTableResult: Completion => Boolean = {
    case Completion.Insert(0 | 1) => true
    case _                        => false
  }

  override def remove(subscriberUrl: SubscriberUrl): F[Boolean] = SessionResource[F].useK {
    measureExecutionTime {
      SqlStatement(name = "migrator - delete")
        .command[SubscriberUrl](sql"""
          DELETE FROM ts_migration
          WHERE subscriber_url = $subscriberUrlEncoder 
            AND status <> '#${Done.value}'
          """.command)
        .arguments(subscriberUrl)
        .build
    } map deleteToTableResult
  }

  private lazy val deleteToTableResult: Completion => Boolean = {
    case Completion.Delete(_) => true
    case _                    => false
  }

  private lazy val falseForForeignKeyViolation: PartialFunction[Throwable, Kleisli[F, Session[F], Boolean]] = {
    case SqlState.ForeignKeyViolation(_) => Kleisli.pure(false)
  }
}

private object MigrationSubscriberTracker {
  def apply[F[_]: MonadCancelThrow: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[MigrationSubscriberTracker[F]] = MonadCancelThrow[F].catchNonFatal {
    new MigrationSubscriberTracker(queriesExecTimes)
  }
}
