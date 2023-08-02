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
package redoprojecttransformation

import cats.effect.Async
import cats.syntax.all._
import io.renku.db.DbClient
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.api.events.StatusChangeEvent.RedoProjectTransformation
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DBUpdater}
import io.renku.eventlog.metrics.QueriesExecutionTimes

import java.time.Instant

trait DequeuedEventHandler[F[_]] extends DBUpdater[F, RedoProjectTransformation]

object DequeuedEventHandler {
  def apply[F[_]: Async: QueriesExecutionTimes]: F[DequeuedEventHandler[F]] =
    new DequeuedEventHandlerImpl[F]().pure.widen
}

private class DequeuedEventHandlerImpl[F[_]: Async: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with DequeuedEventHandler[F]
    with TypeSerializers {

  import io.renku.db.SqlStatement
  import io.renku.eventlog.events.producers.minprojectinfo
  import io.renku.graph.model.events.EventStatus.{TriplesGenerated, TriplesStore}
  import io.renku.graph.model.events.{CompoundEventId, EventId, ExecutionDate}
  import io.renku.graph.model.projects
  import skunk._
  import skunk.data.Completion
  import skunk.implicits._

  override def updateDB(event: RedoProjectTransformation): UpdateOp[F] =
    findLatestSuccessfulEvent(event.project.slug) >>= {
      case Some(eventId) => toTriplesGenerated(eventId) flatMapF toDBUpdateResults(eventId, event.project.slug)
      case None          => triggerMinProjectInfoEvent(event.project.slug).as(DBUpdateResults.ForProjects.empty)
    }

  private def findLatestSuccessfulEvent(slug: projects.Slug) = measureExecutionTime {
    SqlStatement
      .named("redo_transformation - find successful")
      .select[projects.Slug, CompoundEventId](sql"""
        SELECT candidate.event_id, candidate.project_id
        FROM (
          SELECT e.event_id, e.project_id
          FROM event e
          JOIN project p ON e.project_id = p.project_id AND p.project_slug = $projectSlugEncoder
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
      .arguments(slug)
      .build(_.option)
  }

  private def toTriplesGenerated(eventId: CompoundEventId) = measureExecutionTime {
    SqlStatement
      .named[F]("redo_transformation - to TriplesGenerated")
      .command[ExecutionDate *: EventId *: projects.GitLabId *: EmptyTuple](
        sql"""UPDATE event
              SET status = '#${TriplesGenerated.value}', execution_date = $executionDateEncoder
              WHERE event_id = $eventIdEncoder 
                AND project_id = $projectIdEncoder 
                AND status = '#${TriplesStore.value}'
               """.command
      )
      .arguments(ExecutionDate(now()) *: eventId.id *: eventId.projectId *: EmptyTuple)
      .build
  }

  private def toDBUpdateResults(eventId: CompoundEventId, slug: projects.Slug): Completion => F[DBUpdateResults] = {
    case Completion.Update(1) =>
      DBUpdateResults
        .ForProjects(slug, Map(TriplesStore -> -1, TriplesGenerated -> 1))
        .pure[F]
        .widen[DBUpdateResults]
    case Completion.Update(0) =>
      DBUpdateResults.ForProjects.empty.pure[F].widen[DBUpdateResults]
    case _ =>
      new Exception(show"Could not change event $eventId to status $TriplesGenerated").raiseError[F, DBUpdateResults]
  }

  private def triggerMinProjectInfoEvent(slug: projects.Slug) = measureExecutionTime {
    SqlStatement
      .named[F]("redo_transformation - trigger ADD_MIN_PROJECT_INFO")
      .command[projects.Slug](
        sql"""DELETE FROM subscription_category_sync_time
              WHERE category_name = '#${minprojectinfo.categoryName.show}'
                AND project_id = (
                  SELECT project_id
                  FROM project
                  WHERE project_slug = $projectSlugEncoder
                )
               """.command
      )
      .arguments(slug)
      .build
      .void
  }

  override def onRollback(event: RedoProjectTransformation): RollbackOp[F] = RollbackOp.empty
}
