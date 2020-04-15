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

package ch.datascience.dbeventlog.statuschange.commands

import java.time.Instant

import ch.datascience.dbeventlog.EventStatus
import ch.datascience.dbeventlog.EventStatus.{New, Processing}
import ch.datascience.graph.model.events.CompoundEventId
import doobie.implicits._
import doobie.util.fragment
import eu.timepit.refined.api.Refined

private[statuschange] final case class ToNew(
    eventId: CompoundEventId,
    now:     () => Instant = () => Instant.now
) extends ChangeStatusCommand {

  override val status: EventStatus = New

  override def query: fragment.Fragment =
    sql"""|update event_log 
          |set status = $status, execution_date = ${now()}
          |where event_id = ${eventId.id} and project_id = ${eventId.projectId} and status = ${Processing: EventStatus}
          |""".stripMargin

  override def mapResult: Int => UpdateResult = {
    case 0 => UpdateResult.Conflict
    case 1 => UpdateResult.Updated
    case _ => UpdateResult.Failure(Refined.unsafeApply(s"An attempt to set status $status on $eventId"))
  }
}
