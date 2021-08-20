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

package ch.datascience.events.consumers

import cats.Show
import cats.implicits.showInterpolator
import ch.datascience.graph.model.projects
import io.circe.Json

final case class Project(id: projects.Id, path: projects.Path)

object Project {
  implicit lazy val show: Show[Project] = Show.show(project => show"${project.id}, ${project.path}")
}

sealed trait EventSchedulingResult extends Product with Serializable

object EventSchedulingResult {
  case object Accepted             extends EventSchedulingResult
  case object Busy                 extends EventSchedulingResult
  case object UnsupportedEventType extends EventSchedulingResult
  case object BadRequest           extends EventSchedulingResult
  final case class SchedulingError(throwable: Throwable) extends EventSchedulingResult
}

case class EventRequestContent(event: Json, maybePayload: Option[String])

object EventRequestContent {
  def apply(event: Json): EventRequestContent = EventRequestContent(event, None)
}
