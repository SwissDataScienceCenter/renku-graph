/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog

import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects.Path
import doobie.implicits._

trait EventLogDataProvisioning {
  self: InMemoryEventLogDb =>

  def upsertProject(compoundEventId: CompoundEventId, projectPath: Path, eventDate: EventDate): Unit = execute {
    sql"""|INSERT INTO
          |project (project_id, project_path, latest_event_date)
          |VALUES (${compoundEventId.projectId}, $projectPath, $eventDate)
          |ON CONFLICT (project_id)
          |DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
      """.stripMargin.update.run.map(_ => ())
  }
}
