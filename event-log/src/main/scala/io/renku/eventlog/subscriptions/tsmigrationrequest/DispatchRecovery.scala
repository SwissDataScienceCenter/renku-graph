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

import cats.MonadThrow
import cats.effect.Async
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions.DispatchRecovery
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.subscriptions.tsmigrationrequest.MigrationStatus.{New, NonRecoverableFailure, Sent}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.http.server.version.ServiceVersion
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.util.control.NonFatal

private object DispatchRecovery {
  def apply[F[_]: Async: SessionResource: Logger](
      queriesExecTimes: LabeledHistogram[F]
  ): F[DispatchRecovery[F, MigrationRequestEvent]] = MonadThrow[F].catchNonFatal {
    new DispatchRecoveryImpl[F](queriesExecTimes)
  }
}

private class DispatchRecoveryImpl[F[_]: Async: SessionResource: Logger](queriesExecTimes: LabeledHistogram[F],
                                                                         now: () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with subscriptions.DispatchRecovery[F, MigrationRequestEvent]
    with TypeSerializers {
  import cats.syntax.all._
  import skunk._
  import skunk.data.Completion
  import skunk.implicits._

  override def returnToQueue(event: MigrationRequestEvent, reason: SendingResult): F[Unit] = SessionResource[F].useK {
    reason match {
      case SendingResult.TemporarilyUnavailable => updateDate(event, newDate = ChangeDate(now()))
      case _                                    => updateStatus(event, newStatus = New)
    }

  }

  private def updateStatus(event: MigrationRequestEvent, newStatus: MigrationStatus) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - back to queue")
      .command[MigrationStatus ~ ChangeDate ~ SubscriberUrl ~ ServiceVersion](sql"""
          UPDATE ts_migration
          SET status = $migrationStatusEncoder, change_date = $changeDateEncoder
          WHERE subscriber_url = $subscriberUrlEncoder 
            AND subscriber_version = $serviceVersionEncoder
            AND status = '#${Sent.value}'
        """.command)
      .arguments(newStatus ~ ChangeDate(now()) ~ event.subscriberUrl ~ event.subscriberVersion)
      .build
      .flatMapResult {
        case Completion.Update(0 | 1) => ().pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: ${event.show} cannot change status to $newStatus due to: $completion")
            .raiseError[F, Unit]
      }
  }

  private def updateDate(event: MigrationRequestEvent, newDate: ChangeDate) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - bump date")
      .command[ChangeDate ~ SubscriberUrl ~ ServiceVersion](sql"""
          UPDATE ts_migration
          SET change_date = $changeDateEncoder
          WHERE subscriber_url = $subscriberUrlEncoder 
            AND subscriber_version = $serviceVersionEncoder
            AND status = '#${Sent.value}'
        """.command)
      .arguments(newDate ~ event.subscriberUrl ~ event.subscriberVersion)
      .build
      .flatMapResult {
        case Completion.Update(0 | 1) => ().pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: ${event.show} cannot update changeDate to $newDate due to: $completion")
            .raiseError[F, Unit]
      }
  }

  override def recover(url: SubscriberUrl, event: MigrationRequestEvent): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(exception) =>
      Logger[F].info(exception)(s"${categoryName.show}: recovering from NonRecoverable Failure") >>
        SessionResource[F].useK(setFailureStatus(event, exception))
  }

  private def setFailureStatus(event: MigrationRequestEvent, exception: Throwable) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - set failure")
      .command[ChangeDate ~ MigrationMessage ~ SubscriberUrl ~ ServiceVersion](sql"""
          UPDATE ts_migration
          SET status = '#${NonRecoverableFailure.value}', change_date = $changeDateEncoder, message = $migrationMessageEncoder
          WHERE subscriber_url = $subscriberUrlEncoder 
            AND subscriber_version = $serviceVersionEncoder
            AND status = '#${Sent.value}'
        """.command)
      .arguments(ChangeDate(now()) ~ MigrationMessage(exception) ~ event.subscriberUrl ~ event.subscriberVersion)
      .build
      .flatMapResult {
        case Completion.Update(0 | 1) => ().pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: ${event.show} cannot set failure status due to: $completion")
            .raiseError[F, Unit]
      }
  }
}
