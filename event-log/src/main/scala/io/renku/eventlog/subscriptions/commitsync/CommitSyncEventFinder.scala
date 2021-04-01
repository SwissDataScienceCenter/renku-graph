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

package io.renku.eventlog.subscriptions.commitsync

import cats.data.Kleisli
import cats.effect.{Async, Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.{EventFinder, LastSyncedDate, SubscriptionTypeSerializers}
import io.renku.eventlog.{EventDate, EventLogDB}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private class CommitSyncEventFinderImpl[Interpretation[_]: Async: ContextShift: Bracket[*[_], Throwable]](
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, CommitSyncEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): Interpretation[Option[CommitSyncEvent]] = transactor.use { implicit session =>
    session.transaction.use { transaction =>
      for {
        sp <- transaction.savepoint
        result <- findEventAndMarkTaken recoverWith { error =>
                    transaction.rollback(sp).flatMap(_ => error.raiseError[Interpretation, Option[CommitSyncEvent]])
                  }
      } yield result
    }
  }

  private def findEventAndMarkTaken(implicit session: Session[Interpretation]) =
    findEvent flatMap {
      case Some((event, maybeSyncDate)) =>
        setSyncDate(event, maybeSyncDate) map toNoneIfEventAlreadyTaken(event)
      case None => Option.empty[CommitSyncEvent].pure[Interpretation]
    }

  private def findEvent(implicit session: Session[Interpretation]) = measureExecutionTime {
    SqlQuery[Interpretation, Option[(CommitSyncEvent, Option[LastSyncedDate])]](
      Kleisli { session =>
        val query: Query[CategoryName ~ EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate,
                         (CommitSyncEvent, Option[LastSyncedDate])
        ] = sql"""
         SELECT
           (SELECT evt.event_id FROM event evt WHERE evt.project_id = proj.project_id AND evt.event_date = proj.latest_event_date),
           proj.project_id,
           proj.project_path,
           sync_time.last_synced,
           proj.latest_event_date
         FROM project proj
         LEFT JOIN subscription_category_sync_time sync_time
           ON sync_time.project_id = proj.project_id AND sync_time.category_name = $categoryNamePut
         WHERE
           sync_time.last_synced IS NULL
           OR (
                (($eventDatePut - proj.latest_event_date) <= INTERVAL '7 days' AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 hour')
             OR (($eventDatePut - proj.latest_event_date) >  INTERVAL '7 days' AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 day')
           )
         ORDER BY proj.latest_event_date DESC
         LIMIT 1
        """
          .query(compoundEventIdGet ~ projectPathGet ~ lastSyncedDateGet.opt ~ eventDateGet)
          .map { case id ~ projectPath ~ maybeLastSyncDate ~ latestEventDate =>
            CommitSyncEvent(id,
                            projectPath,
                            maybeLastSyncDate getOrElse LastSyncedDate(latestEventDate.value)
            ) -> maybeLastSyncDate
          }
        val eventDate    = EventDate(now())
        val lastSyncDate = LastSyncedDate(now())
        session.prepare(query).use(_.option(categoryName ~ eventDate ~ lastSyncDate ~ eventDate ~ lastSyncDate))
      },
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find event")
    )
  }

  private def setSyncDate(event: CommitSyncEvent, maybeSyncedDate: Option[LastSyncedDate])(implicit
      session:                   Session[Interpretation]
  ) =
    if (maybeSyncedDate.isDefined) updateLastSyncedDate(event)
    else insertLastSyncedDate(event)

  private def updateLastSyncedDate(event: CommitSyncEvent)(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[LastSyncedDate ~ projects.Id ~ CategoryName] =
            sql"""UPDATE subscription_category_sync_time
                  SET last_synced = $lastSyncedDatePut
                  WHERE project_id = $projectIdPut AND category_name = $categoryNamePut
              """.command
          session.prepare(query).use(_.execute(LastSyncedDate(now()) ~ event.id.projectId ~ categoryName))
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - update last_synced")
      )
    }

  private def insertLastSyncedDate(event: CommitSyncEvent)(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[projects.Id ~ CategoryName ~ LastSyncedDate] = sql"""
          INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
          VALUES ($projectIdPut, $categoryNamePut, $lastSyncedDatePut)
          ON CONFLICT (project_id, category_name)
          DO UPDATE 
          SET last_synced = EXCLUDED.last_synced
          """.command
          session.prepare(query).use(_.execute(event.id.projectId ~ categoryName ~ LastSyncedDate(now())))
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - insert last_synced")
      )
    }

  private def toNoneIfEventAlreadyTaken(event: CommitSyncEvent): Completion => Option[CommitSyncEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }

}

private object CommitSyncEventFinder {
  def apply(
      transactor:       SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  )(implicit ME:        Bracket[IO, Throwable], contextShift: ContextShift[IO]): IO[EventFinder[IO, CommitSyncEvent]] = IO {
    new CommitSyncEventFinderImpl(transactor, queriesExecTimes)
  }
}
