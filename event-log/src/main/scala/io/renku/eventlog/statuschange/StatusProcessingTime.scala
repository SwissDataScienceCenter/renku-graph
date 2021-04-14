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

import ch.datascience.db.SqlQuery
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.TypeSerializers

trait StatusProcessingTime extends TypeSerializers {
  def upsertStatusProcessingTime(eventId:             CompoundEventId,
                                 status:              EventStatus,
                                 maybeProcessingTime: Option[EventProcessingTime]
  ): Option[SqlQuery[Int]] = maybeProcessingTime.map { processingTime =>
    SqlQuery[Int](
      query = sql"""|INSERT INTO status_processing_time(event_id, project_id, status, processing_time)
                    |VALUES(${eventId.id}, ${eventId.projectId}, $status, $processingTime)
                    |ON CONFLICT (event_id, project_id, status)
                    |DO UPDATE SET processing_time = EXCLUDED.processing_time;
                    |""".stripMargin.update.run.map(_ => 1),
      name = "upsert_processing_time"
    )
  }
}
