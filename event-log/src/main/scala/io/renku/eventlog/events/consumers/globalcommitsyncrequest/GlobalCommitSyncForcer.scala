/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.globalcommitsyncrequest

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.events.producers.{SubscriptionTypeSerializers, globalcommitsync}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.{EventDate, LastSyncedDate}
import io.renku.graph.model.projects
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private trait GlobalCommitSyncForcer[F[_]] {
  def moveGlobalCommitSync(projectId: projects.GitLabId, projectSlug: projects.Slug): F[Unit]
}

private class GlobalCommitSyncForcerImpl[F[_]: Async: SessionResource: QueriesExecutionTimes](
    syncFrequency:  Duration,
    delayOnRequest: Duration,
    now:            () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with GlobalCommitSyncForcer[F]
    with TypeSerializers
    with SubscriptionTypeSerializers {

  override def moveGlobalCommitSync(projectId: projects.GitLabId, projectSlug: projects.Slug): F[Unit] =
    SessionResource[F].useK {
      scheduleGlobalSync(projectId) >>= {
        case true  => upsertProject(projectId, projectSlug)
        case false => Kleisli.pure(())
      }
    }

  private def scheduleGlobalSync(projectId: projects.GitLabId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - move last_synced")
      .command[LastSyncedDate *: projects.GitLabId *: EmptyTuple](sql"""
        UPDATE subscription_category_sync_time
        SET last_synced = $lastSyncedDateEncoder
        WHERE project_id = $projectIdEncoder AND category_name = '#${globalcommitsync.categoryName.show}'
        """.command)
      .arguments(LastSyncedDate(now().minus(syncFrequency).plus(delayOnRequest)) *: projectId *: EmptyTuple)
      .build
      .mapResult {
        case Completion.Update(0) => true
        case _                    => false
      }
  }

  private def upsertProject(projectId: projects.GitLabId, projectSlug: projects.Slug) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - insert project")
      .command[projects.GitLabId *: projects.Slug *: EventDate *: EmptyTuple](sql"""
        INSERT INTO project (project_id, project_slug, latest_event_date)
        VALUES ($projectIdEncoder, $projectSlugEncoder, $eventDateEncoder)
        ON CONFLICT (project_id) DO NOTHING
        """.command)
      .arguments(projectId *: projectSlug *: EventDate(Instant.EPOCH) *: EmptyTuple)
      .build
      .void
  }
}

private object GlobalCommitSyncForcer {
  import io.renku.config.ConfigLoader._

  import scala.concurrent.duration.FiniteDuration

  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes](
      config: Config = ConfigFactory.load()
  ): F[GlobalCommitSyncForcer[F]] = for {
    configFrequency      <- find[F, FiniteDuration]("global-commit-sync-frequency", config)
    syncFrequency        <- Duration.ofDays(configFrequency.toDays).pure[F]
    configDelayOnRequest <- find[F, FiniteDuration]("global-commit-sync-delay-on-request", config)
    delayOnRequest       <- Duration.ofMinutes(configDelayOnRequest.toMinutes).pure[F]
  } yield new GlobalCommitSyncForcerImpl(syncFrequency, delayOnRequest)
}
