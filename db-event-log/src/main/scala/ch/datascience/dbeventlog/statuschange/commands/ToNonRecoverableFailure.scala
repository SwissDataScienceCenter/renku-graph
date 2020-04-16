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

import ch.datascience.dbeventlog.EventStatus.{NonRecoverableFailure, Processing}
import ch.datascience.dbeventlog.{EventMessage, EventStatus}
import ch.datascience.graph.model.events.CompoundEventId
import doobie.implicits._
import doobie.util.fragment.Fragment

final case class ToNonRecoverableFailure(
    eventId:      CompoundEventId,
    maybeMessage: Option[EventMessage],
    now:          () => Instant = () => Instant.now
) extends ChangeStatusCommand {

  override val status: EventStatus = NonRecoverableFailure

  override def query: Fragment =
    sql"""|update event_log 
          |set status = $status, execution_date = ${now()}, message = $maybeMessage
          |where event_id = ${eventId.id} and project_id = ${eventId.projectId} and status = ${Processing: EventStatus}
          |""".stripMargin
}
