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

package io.renku.eventlog.events.producers
package projectsync

import EventsSender.SendingResult
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers
import io.renku.eventlog.events.producers.{DispatchRecovery, SubscriptionTypeSerializers}
import io.renku.events.CategoryName
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import skunk.data.Completion

import scala.util.control.NonFatal

private class DispatchRecoveryImpl[F[_]: MonadCancelThrow: SessionResource: Logger](
    queriesExecTimes: LabeledHistogram[F]
) extends DbClient(Some(queriesExecTimes))
    with producers.DispatchRecovery[F, ProjectSyncEvent]
    with SubscriptionTypeSerializers {

  import skunk._
  import skunk.implicits._

  override def returnToQueue(event: ProjectSyncEvent, reason: SendingResult): F[Unit] = removeRow(event)

  override def recover(url: SubscriberUrl, event: ProjectSyncEvent): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(exception) =>
      removeRow(event) >> Logger[F].error(exception)(show"$categoryName: $event, url = $url -> event will be retried")
  }

  private def removeRow(event: ProjectSyncEvent) = SessionResource[F].useK {
    measureExecutionTime {
      SqlStatement
        .named(s"${categoryName.value.toLowerCase} - dispatch recovery")
        .command[projects.Id ~ CategoryName](sql"""
          DELETE FROM subscription_category_sync_time
          WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
          """.command)
        .arguments(event.projectId ~ categoryName)
        .build
        .flatMapResult {
          case Completion.Delete(0 | 1) => ().pure[F]
          case completion =>
            new Exception(s"${categoryName.show}: deleting row failed with code $completion")
              .raiseError[F, Unit]
        }
    }
  }
}

private object DispatchRecovery {
  def apply[F[_]: MonadCancelThrow: SessionResource: Logger](
      queriesExecTimes: LabeledHistogram[F]
  ): F[DispatchRecovery[F, ProjectSyncEvent]] = MonadCancelThrow[F].catchNonFatal {
    new DispatchRecoveryImpl[F](queriesExecTimes)
  }
}
