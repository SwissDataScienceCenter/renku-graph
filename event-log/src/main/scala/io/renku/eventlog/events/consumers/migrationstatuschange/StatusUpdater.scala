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

package io.renku.eventlog.events.consumers.migrationstatuschange

import cats.effect.Async
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.MigrationStatus.Sent
import io.renku.eventlog.events.consumers.migrationstatuschange.Event.{ToNonRecoverableFailure, ToRecoverableFailure}
import io.renku.eventlog._
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.metrics.LabeledHistogram

import java.time.Instant

private trait StatusUpdater[F[_]] {
  def updateStatus(event: Event): F[Unit]
}

private class StatusUpdaterImpl[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F],
                                                              now: () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with StatusUpdater[F]
    with TSMigtationTypeSerializers {
  import skunk._
  import skunk.data.Completion
  import skunk.implicits._

  def updateStatus(event: Event): F[Unit] = SessionResource[F].useK {
    update(event)
  }

  private def update(event: Event) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - update")
      .command[MigrationStatus ~ ChangeDate ~ Option[MigrationMessage] ~ SubscriberUrl ~ ServiceVersion](sql"""
        UPDATE ts_migration
        SET status = $migrationStatusEncoder, change_date = $changeDateEncoder, message = ${migrationMessageEncoder.opt}
        WHERE subscriber_url = $subscriberUrlEncoder 
          AND subscriber_version = $serviceVersionEncoder
          AND status = '#${Sent.value}'
        """.command)
      .arguments(
        event.newStatus ~ ChangeDate(now()) ~ getMessage(event) ~ event.subscriberUrl ~ event.subscriberVersion
      )
      .build
      .flatMapResult {
        case Completion.Update(0 | 1) => ().pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: cannot update TS migration record: $completion").raiseError[F, Unit]
      }
  }

  private def getMessage: Event => Option[MigrationMessage] = {
    case ToNonRecoverableFailure(_, _, message) => message.some
    case ToRecoverableFailure(_, _, message)    => message.some
    case _                                      => None
  }
}

private object StatusUpdater {
  def apply[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[StatusUpdater[F]] =
    new StatusUpdaterImpl[F](queriesExecTimes).pure[F].widen[StatusUpdater[F]]
}
