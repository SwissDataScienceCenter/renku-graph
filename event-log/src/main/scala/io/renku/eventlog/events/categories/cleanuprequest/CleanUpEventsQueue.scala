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

package io.renku.eventlog.events.categories.cleanuprequest

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk.data.Completion

import java.time.OffsetDateTime

private trait CleanUpEventsQueue[F[_]] {
  def offer(projectId: projects.Id, projectPath: projects.Path): F[Unit]
}

private object CleanUpEventsQueue {
  def apply[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[CleanUpEventsQueue[F]] =
    MonadThrow[F].catchNonFatal(new CleanUpEventsQueueImpl[F](queriesExecTimes))
}

private class CleanUpEventsQueueImpl[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F],
                                                                   now: () => OffsetDateTime = () => OffsetDateTime.now
) extends DbClient[F](Some(queriesExecTimes))
    with CleanUpEventsQueue[F]
    with TypeSerializers {

  import eu.timepit.refined.auto._
  import skunk._
  import skunk.codec.all._
  import skunk.implicits._

  override def offer(projectId: projects.Id, projectPath: projects.Path): F[Unit] = SessionResource[F].useK {
    measureExecutionTime {
      SqlStatement[F](name = "clean_up_events_queue - offer")
        .command[OffsetDateTime ~ projects.Id ~ projects.Path](
          sql"""INSERT INTO clean_up_events_queue (date, project_id, project_path)
                VALUES ($timestamptz, $projectIdEncoder, $projectPathEncoder)
                ON CONFLICT DO NOTHING
          """.command
        )
        .arguments(now() ~ projectId ~ projectPath)
        .build
    } flatMapF {
      case Completion.Insert(0 | 1) => ().pure[F]
      case other => new Exception(show"$categoryName: offering $projectPath failed with $other").raiseError[F, Unit]
    }
  }
}
