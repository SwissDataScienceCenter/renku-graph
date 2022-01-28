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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import cats.{Id, MonadThrow}
import eu.timepit.refined.api.Refined
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.globalcommitsync.GlobalCommitSyncEvent.{CommitsCount, CommitsInfo}
import io.renku.eventlog.subscriptions.globalcommitsync.GlobalCommitSyncEventFinder.syncInterval
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus.AwaitingDeletion
import io.renku.graph.model.events.{CategoryName, CommitId, EventStatus, LastSyncedDate}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private class GlobalCommitSyncEventFinderImpl[F[_]: Async](
    sessionResource:       SessionResource[F, EventLogDB],
    lastSyncedDateUpdater: LastSyncedDateUpdater[F],
    queriesExecTimes:      LabeledHistogram[F, SqlStatement.Name],
    now:                   () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, GlobalCommitSyncEvent]
    with SubscriptionTypeSerializers {

  import skunk.codec.all.int8

  override def popEvent(): F[Option[GlobalCommitSyncEvent]] = sessionResource.useK(findEventAndMarkTaken)

  private def findEventAndMarkTaken =
    findProject >>= updateLastSyncDate >>= findCommitsInfo

  private def updateLastSyncDate(
      maybeProject: Option[(Project, Option[LastSyncedDate])]
  ): Kleisli[F, Session[F], Option[(Project, Option[LastSyncedDate])]] = maybeProject match {
    case Some(projectAndMaybeLastSync @ (project, _)) =>
      Kleisli.liftF(
        lastSyncedDateUpdater
          .run(project.id, LastSyncedDate(now()).some)
          .map(toNoneIfEventAlreadyTaken(projectAndMaybeLastSync)(_))
      )
    case None => Kleisli.pure(Option.empty[(Project, Option[LastSyncedDate])])
  }

  private def findProject = measureExecutionTime {
    val lastSyncDate = LastSyncedDate(now())
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find project"))
      .select[CategoryName ~ LastSyncedDate, (Project, Option[LastSyncedDate])](
        sql"""
              SELECT
                proj.project_id,
                proj.project_path,
                sync_time.last_synced
              FROM project proj
              LEFT JOIN subscription_category_sync_time sync_time
                ON proj.project_id = sync_time.project_id AND sync_time.category_name = $categoryNameEncoder
              WHERE
                sync_time.last_synced IS NULL 
                OR  (($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '#${syncInterval.toDays.toString} days')
              ORDER BY proj.latest_event_date DESC
              LIMIT 1"""
          .query(projectDecoder ~ lastSyncedDateDecoder.opt)
          .map { case project ~ lastSyncedDate => (project, lastSyncedDate) }
      )
      .arguments(categoryName ~ lastSyncDate)
      .build(_.option)
  }

  private def findCommitsInfo(maybeProjectAndLastSyncedDate: Option[(Project, Option[LastSyncedDate])]) =
    maybeProjectAndLastSyncedDate match {
      case Some((project, maybeLastSyncedDate)) =>
        measureExecutionTime {
          SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find commits"))
            .select[projects.Id ~ EventStatus ~ projects.Id ~ EventStatus, (Long, Option[CommitId])](
              sql"""
                   SELECT
                     (SELECT COUNT(event_id) FROM event WHERE project_id = $projectIdEncoder AND status <> $eventStatusEncoder) AS count,
                     (SELECT event_id FROM event WHERE project_id = $projectIdEncoder AND status <> $eventStatusEncoder ORDER BY event_date DESC LIMIT 1) AS latest
                 """.query(int8 ~ commitIdDecoder.opt)
            )
            .arguments(project.id ~ AwaitingDeletion ~ project.id ~ AwaitingDeletion)
            .build[Id](_.unique)
            .mapResult(toEvent(project, maybeLastSyncedDate))
        }
      case None => Kleisli.pure(Option.empty[GlobalCommitSyncEvent])
    }

  private def toEvent(project:             Project,
                      maybeLastSyncedDate: Option[LastSyncedDate]
  ): Id[(Long, Option[CommitId])] => Option[GlobalCommitSyncEvent] = {
    case (commitsCount, Some(latestCommitId)) =>
      GlobalCommitSyncEvent(project, CommitsInfo(CommitsCount(commitsCount), latestCommitId), maybeLastSyncedDate).some
    case _ => Option.empty[GlobalCommitSyncEvent]
  }

  private def toNoneIfEventAlreadyTaken(
      projectAndMaybeLastSync: (Project, Option[LastSyncedDate])
  ): Completion => Option[(Project, Option[LastSyncedDate])] = {
    case Completion.Update(1) | Completion.Insert(1) => Some(projectAndMaybeLastSync)
    case _                                           => None
  }
}

private object GlobalCommitSyncEventFinder {
  val syncInterval = Duration.ofDays(7)

  def apply[F[_]: Async](
      sessionResource:       SessionResource[F, EventLogDB],
      lastSyncedDateUpdater: LastSyncedDateUpdater[F],
      queriesExecTimes:      LabeledHistogram[F, SqlStatement.Name]
  ): F[EventFinder[F, GlobalCommitSyncEvent]] = MonadThrow[F].catchNonFatal(
    new GlobalCommitSyncEventFinderImpl(sessionResource, lastSyncedDateUpdater, queriesExecTimes)
  )
}
