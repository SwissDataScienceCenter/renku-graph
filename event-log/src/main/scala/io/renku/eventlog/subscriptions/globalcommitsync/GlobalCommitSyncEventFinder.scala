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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.data.Kleisli
import cats.effect.{BracketThrow, IO, Sync}
import cats.syntax.all._
import ch.datascience.db.implicits._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.EventStatus.AwaitingDeletion
import ch.datascience.graph.model.events.{CategoryName, CommitId, EventStatus, LastSyncedDate}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.globalcommitsync.GlobalCommitSyncEventFinder.syncInterval
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private class GlobalCommitSyncEventFinderImpl[Interpretation[_]: BracketThrow: Sync](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, GlobalCommitSyncEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): Interpretation[Option[GlobalCommitSyncEvent]] = sessionResource.useK(findEventAndMarkTaken)

  private def findEventAndMarkTaken =
    findProject >>= findCommits >>= {
      case Some((project, commits)) =>
        val event = GlobalCommitSyncEvent(project, commits)
        updateLastSyncedDate(event) map toNoneIfEventAlreadyTaken(event)
      case None => Kleisli.pure(Option.empty[GlobalCommitSyncEvent])
    }

  private def findProject = measureExecutionTime {
    val lastSyncDate = LastSyncedDate(now())
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find project"))
      .select[CategoryName ~ LastSyncedDate ~ LastSyncedDate, Project](
        sql"""
              SELECT
                proj.project_id,
                proj.project_path
              FROM project proj
              LEFT JOIN subscription_category_sync_time sync_time
                ON proj.project_id = sync_time.project_id AND sync_time.category_name = $categoryNameEncoder
              LEFT JOIN event evt
                ON proj.project_id = evt.project_id AND ( ($lastSyncedDateEncoder - evt.event_date) > INTERVAL '#${syncInterval.toDays.toString} days')
              WHERE
                (sync_time.last_synced IS NULL AND evt.event_id IS NOT NULL)
                OR  (($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '#${syncInterval.toDays.toString} days')
              ORDER BY proj.latest_event_date DESC
              LIMIT 1"""
          .query(projectDecoder)
      )
      .arguments(categoryName ~ lastSyncDate ~ lastSyncDate)
      .build(_.option)
  }

  private def findCommits(maybeProject: Option[Project]) =
    maybeProject match {
      case Some(project) =>
        measureExecutionTime {
          SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find commits"))
            .select[projects.Id ~ EventStatus, CommitId](
              sql"""
                   SELECT evt.event_id
                   FROM event evt
                   WHERE evt.project_id = $projectIdEncoder AND evt.status <> $eventStatusEncoder
                 """.query(commitIdDecoder)
            )
            .arguments(project.id ~ AwaitingDeletion)
            .build(_.toList)
            .mapResult {
              case Nil     => Option.empty[(Project, List[CommitId])]
              case commits => Some((project, commits))
            }
        }
      case None => Kleisli.pure(Option.empty[(Project, List[CommitId])])
    }

  private def updateLastSyncedDate(event: GlobalCommitSyncEvent) =
    measureExecutionTime {
      SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - update last_synced"))
        .command[projects.Id ~ CategoryName ~ LastSyncedDate](
          sql"""INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
                VALUES ( $projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
                ON CONFLICT (project_id, category_name)
                DO UPDATE SET last_synced = EXCLUDED.last_synced
            """.command
        )
        .arguments(event.projectId ~ categoryName ~ LastSyncedDate(now()))
        .build
    }

  private implicit class SyncEventOps(event: GlobalCommitSyncEvent) {
    lazy val projectId: projects.Id = event.project.id
  }

  private def toNoneIfEventAlreadyTaken(event: GlobalCommitSyncEvent): Completion => Option[GlobalCommitSyncEvent] = {
    case Completion.Update(1) | Completion.Insert(1) => Some(event)
    case _                                           => None
  }

}

private object GlobalCommitSyncEventFinder {
  val syncInterval = Duration.ofDays(7)

  def apply(
      sessionResource:  SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name]
  ): IO[EventFinder[IO, GlobalCommitSyncEvent]] = IO {
    new GlobalCommitSyncEventFinderImpl(sessionResource, queriesExecTimes)
  }
}
