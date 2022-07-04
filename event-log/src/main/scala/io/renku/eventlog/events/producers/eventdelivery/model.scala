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

package io.renku.eventlog.events.producers.eventdelivery

import io.renku.graph.model.projects
import cats.Show
import cats.syntax.all._
import io.renku.graph.model.events

sealed trait EventTypeId {
  def value: String
}

case object DeletingProjectTypeId extends EventTypeId {
  override val value: String = "DELETING"
}

sealed trait EventDeliveryId {
  def projectId: projects.Id
}

final case class CompoundEventDeliveryId(compoundEventId: events.CompoundEventId) extends EventDeliveryId {
  def projectId: projects.Id = compoundEventId.projectId
}

object CompoundEventDeliveryId {
  implicit lazy val show: Show[CompoundEventDeliveryId] = eventId => eventId.compoundEventId.show
}

final case class DeletingProjectDeliverId(projectId: projects.Id) extends EventDeliveryId {
  def eventTypeId:            EventTypeId = DeletingProjectTypeId
  override lazy val toString: String      = s"deleting project delivery event, projectId = $projectId"
}

object DeletingProjectDeliverId {
  implicit lazy val show: Show[DeletingProjectDeliverId] =
    Show.show(id => show"projectId = ${id.projectId}")
}
