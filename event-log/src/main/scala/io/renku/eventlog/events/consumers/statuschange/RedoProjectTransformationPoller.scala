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

package io.renku.eventlog.events.consumers.statuschange

import StatusChangeEvent.RedoProjectTransformation
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.DbClient
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.metrics.QueriesExecutionTimes

import java.time.Instant

private trait RedoProjectTransformationPoller[F[_]] extends DBUpdater[F, RedoProjectTransformation]

private object RedoProjectTransformationPoller {
  def apply[F[_]: Async: QueriesExecutionTimes]: F[RedoProjectTransformationPoller[F]] =
    new RedoProjectTransformationPollerImpl[F]().pure.widen
}

private class RedoProjectTransformationPollerImpl[F[_]: Async: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with RedoProjectTransformationPoller[F]
    with TypeSerializers {

  import io.renku.db.SqlStatement
  import io.renku.eventlog.events.producers.minprojectinfo
  import io.renku.graph.model.events.EventStatus.{TriplesGenerated, TriplesStore}
  import io.renku.graph.model.events.{CompoundEventId, EventId, ExecutionDate}
  import io.renku.graph.model.projects
  import skunk.data.Completion
  import skunk.implicits._
  import skunk.{Session, ~}

  override def updateDB(event: RedoProjectTransformation): UpdateResult[F] =
    findLatestSuccessfulEvent(event.projectPath) >>= {
      case Some(eventId) => toTriplesGenerated(eventId) flatMapF toDBUpdateResults(eventId, event.projectPath)
      case None          => triggerMinProjectInfoEvent(event.projectPath) map (_ => DBUpdateResults.ForProjects.empty)
    }

  private def findLatestSuccessfulEvent(path: projects.Path) = measureExecutionTime {
    SqlStatement
      .named("redo_transformation - find successful")
      .select[projects.Path, CompoundEventId](sql"""
        SELECT candidate.event_id, candidate.project_id
        FROM (
          SELECT e.event_id, e.project_id
          FROM event e
          JOIN project p ON e.project_id = p.project_id AND p.project_path = $projectPathEncoder
          WHERE e.status = '#${TriplesStore.value}'
          ORDER BY event_date DESC
          LIMIT 1
        ) candidate
        JOIN LATERAL (
          SELECT COUNT(ee.event_id) AS count
          FROM event ee
          WHERE ee.project_id = candidate.project_id
            AND ee.status = '#${TriplesGenerated.value}'
        ) triples_generated ON 1 = 1
        WHERE triples_generated.count = 0
        """.query(compoundEventIdDecoder))
      .arguments(path)
      .build(_.option)
  }

  private def toTriplesGenerated(eventId: CompoundEventId) = measureExecutionTime {
    SqlStatement
      .named[F]("redo_transformation - to TriplesGenerated")
      .command[ExecutionDate ~ EventId ~ projects.GitLabId](
        sql"""UPDATE event
              SET status = '#${TriplesGenerated.value}', execution_date = $executionDateEncoder
              WHERE event_id = $eventIdEncoder 
                AND project_id = $projectIdEncoder 
                AND status = '#${TriplesStore.value}'
               """.command
      )
      .arguments(ExecutionDate(now()) ~ eventId.id ~ eventId.projectId)
      .build
  }

  private def toDBUpdateResults(eventId: CompoundEventId, path: projects.Path): Completion => F[DBUpdateResults] = {
    case Completion.Update(1) =>
      DBUpdateResults
        .ForProjects(path, Map(TriplesStore -> -1, TriplesGenerated -> 1))
        .pure[F]
        .widen[DBUpdateResults]
    case Completion.Update(0) =>
      DBUpdateResults.ForProjects.empty.pure[F].widen[DBUpdateResults]
    case _ =>
      new Exception(show"Could not change event $eventId to status $TriplesGenerated").raiseError[F, DBUpdateResults]
  }

  private def triggerMinProjectInfoEvent(path: projects.Path) = measureExecutionTime {
    SqlStatement
      .named[F]("redo_transformation - trigger ADD_MIN_PROJECT_INFO")
      .command[projects.Path](
        sql"""DELETE FROM subscription_category_sync_time
              WHERE category_name = '#${minprojectinfo.categoryName.show}' 
                AND project_id = (
                  SELECT project_id
                  FROM project
                  WHERE project_path = $projectPathEncoder
                )
               """.command
      )
      .arguments(path)
      .build
      .void
  }

  override def onRollback(event: RedoProjectTransformation): Kleisli[F, Session[F], Unit] = Kleisli.pure(())
}
