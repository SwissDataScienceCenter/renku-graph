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
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions.tsmigrationrequest.MigrationStatus._
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.http.server.version.ServiceVersion
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private class EventFinder[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F],
                                                        now: () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with subscriptions.EventFinder[F, MigrationRequestEvent]
    with TypeSerializers {

  import EventFinder._

  override def popEvent(): F[Option[MigrationRequestEvent]] = SessionResource[F].useK {
    findEvent >>= {
      case Some(event) => markTaken(event) map toNoneIfTaken(event)
      case _           => Kleisli.pure(None)
    }
  }

  type SubscriptionRecord = (SubscriberUrl, ServiceVersion, MigrationStatus, ChangeDate)

  private def findEvent = findEvents.map(findCandidate).map(toEvent)

  private def findEvents = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - find events")
      .select[Void, SubscriptionRecord](
        sql"""SELECT m.subscriber_url, m.subscriber_version, m.status, m.change_date
              FROM (
                SELECT subscriber_version
                FROM ts_migration
                ORDER BY change_date DESC
                LIMIT 1
              ) latest 
              JOIN ts_migration m on m.subscriber_version = latest.subscriber_version 
              ORDER BY m.change_date DESC
      """.query(subscriberUrlDecoder ~ serviceVersionDecoder ~ migrationStatusDecoder ~ changeDateDecoder)
          .map { case url ~ version ~ status ~ changeDate => (url, version, status, changeDate) }
      )
      .arguments(Void)
      .build(_.toList)
  }

  private lazy val findCandidate: List[SubscriptionRecord] => Option[SubscriptionRecord] = {
    case Nil => None
    case records =>
      val groupedByStatus = records.groupBy(_._3)
      if (groupedByStatus contains MigrationStatus.Done) None
      else if (groupedByStatus contains MigrationStatus.Sent)
        groupedByStatus(MigrationStatus.Sent).find(olderThan(SentStatusTimeout))
      else groupedByStatus.get(MigrationStatus.New).flatMap(_.sortBy(_._4.value).reverse.headOption)
  }

  private def olderThan(duration: Duration): SubscriptionRecord => Boolean = { case (_, _, _, date) =>
    (Duration.between(date.value, now()) compareTo duration) >= 0
  }

  private lazy val toEvent: Option[SubscriptionRecord] => Option[MigrationRequestEvent] =
    _.map { case (url, version, _, _) => MigrationRequestEvent(url, version) }

  private def markTaken(event: MigrationRequestEvent) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - mark taken")
      .command[ChangeDate ~ SubscriberUrl ~ ServiceVersion](sql"""
        UPDATE ts_migration
        SET status = '#${Sent.value}', change_date = $changeDateEncoder
        WHERE subscriber_url = $subscriberUrlEncoder 
          AND subscriber_version = $serviceVersionEncoder
        """.command)
      .arguments(ChangeDate(now()) ~ event.subscriberUrl ~ event.subscriberVersion)
      .build
      .flatMapResult {
        case Completion.Update(1) => true.pure[F]
        case Completion.Update(0) => false.pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: cannot update TS migration record: $completion").raiseError[F, Boolean]
      }
  }

  private def toNoneIfTaken(event: MigrationRequestEvent): Boolean => Option[MigrationRequestEvent] = {
    case true  => event.some
    case false => None
  }
}

private object EventFinder {
  val SentStatusTimeout = Duration ofHours 1

  def apply[F[_]: Async: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[subscriptions.EventFinder[F, MigrationRequestEvent]] = MonadThrow[F].catchNonFatal {
    new EventFinder[F](queriesExecTimes)
  }
}
