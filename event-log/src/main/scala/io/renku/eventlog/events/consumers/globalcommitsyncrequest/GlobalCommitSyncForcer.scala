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

package io.renku.eventlog.events.consumers.globalcommitsyncrequest

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers.{SubscriptionTypeSerializers, globalcommitsync}
import io.renku.eventlog.{EventDate, TypeSerializers}
import io.renku.graph.model.events.LastSyncedDate
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private trait GlobalCommitSyncForcer[F[_]] {
  def moveGlobalCommitSync(projectId: projects.Id, projectPath: projects.Path): F[Unit]
}

private class GlobalCommitSyncForcerImpl[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F],
                                                                                  syncFrequency:  Duration,
                                                                                  delayOnRequest: Duration,
                                                                                  now: () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with GlobalCommitSyncForcer[F]
    with TypeSerializers
    with SubscriptionTypeSerializers {

  override def moveGlobalCommitSync(projectId: projects.Id, projectPath: projects.Path): F[Unit] =
    SessionResource[F].useK {
      scheduleGlobalSync(projectId) >>= {
        case true  => upsertProject(projectId, projectPath)
        case false => Kleisli.pure(())
      }
    }

  private def scheduleGlobalSync(projectId: projects.Id) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - move last_synced"))
      .command[LastSyncedDate ~ projects.Id](sql"""
        UPDATE subscription_category_sync_time
        SET last_synced = $lastSyncedDateEncoder
        WHERE project_id = $projectIdEncoder AND category_name = '#${globalcommitsync.categoryName.show}'
        """.command)
      .arguments(LastSyncedDate(now().minus(syncFrequency).plus(delayOnRequest)), projectId)
      .build
      .mapResult {
        case Completion.Update(0) => true
        case _                    => false
      }
  }

  private def upsertProject(projectId: projects.Id, projectPath: projects.Path) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - insert project"))
      .command[projects.Id ~ projects.Path ~ EventDate](sql"""
        INSERT INTO project (project_id, project_path, latest_event_date)
        VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
        ON CONFLICT (project_id)
        DO NOTHING
        """.command)
      .arguments(projectId ~ projectPath ~ EventDate(Instant.EPOCH))
      .build
      .void
  }
}

private object GlobalCommitSyncForcer {
  import io.renku.config.ConfigLoader._

  import scala.concurrent.duration.FiniteDuration

  def apply[F[_]: MonadCancelThrow: SessionResource](
      queriesExecTimes: LabeledHistogram[F],
      config:           Config = ConfigFactory.load()
  ): F[GlobalCommitSyncForcer[F]] = for {
    configFrequency      <- find[F, FiniteDuration]("global-commit-sync-frequency", config)
    syncFrequency        <- Duration.ofDays(configFrequency.toDays).pure[F]
    configDelayOnRequest <- find[F, FiniteDuration]("global-commit-sync-delay-on-request", config)
    delayOnRequest       <- Duration.ofMinutes(configDelayOnRequest.toMinutes).pure[F]
  } yield new GlobalCommitSyncForcerImpl(queriesExecTimes, syncFrequency, delayOnRequest)
}
