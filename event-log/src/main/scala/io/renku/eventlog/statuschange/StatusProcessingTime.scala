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

package io.renku.eventlog.statuschange

import cats.data.Kleisli
import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import eu.timepit.refined.auto._
import io.renku.eventlog.TypeSerializers
import skunk._
import skunk.implicits._
import skunk.codec.all._
import skunk.data.Completion

trait StatusProcessingTime extends TypeSerializers {
  def upsertStatusProcessingTime[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      eventId:             CompoundEventId,
      status:              EventStatus,
      maybeProcessingTime: Option[EventProcessingTime]
  ): Option[SqlQuery[Interpretation, Int]] = maybeProcessingTime.map { processingTime =>
    SqlQuery[Interpretation, Int](
      Kleisli { session =>
        val query: Command[EventId ~ projects.Id ~ EventStatus ~ EventProcessingTime] =
          sql"""INSERT INTO status_processing_time(event_id, project_id, status, processing_time)
                VALUES($eventIdPut, $projectIdPut, $eventStatusPut, $eventProcessingTimePut)
                ON CONFLICT (event_id, project_id, status)
                DO UPDATE SET processing_time = EXCLUDED.processing_time;
                """.command
        session
          .prepare(query)
          .use(_.execute(eventId.id ~ eventId.projectId ~ status ~ processingTime))
          .map(_ => 1)
      },
      name = "upsert_processing_time"
    )
  }
}
