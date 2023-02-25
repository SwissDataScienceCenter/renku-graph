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

package io.renku.events.consumers

import cats.Show
import io.renku.graph.model.projects

final case class Project(id: projects.GitLabId, path: projects.Path)

object Project {
  implicit lazy val show: Show[Project] = Show.show { case Project(id, path) =>
    s"projectId = $id, projectPath = $path"
  }
}

sealed trait EventSchedulingResult extends Product with Serializable {
  def widen: EventSchedulingResult = this
}

object EventSchedulingResult {
  type Accepted = Accepted.type
  case object Accepted                                   extends EventSchedulingResult
  case object Busy                                       extends EventSchedulingResult
  case object UnsupportedEventType                       extends EventSchedulingResult
  case object BadRequest                                 extends EventSchedulingResult
  final case class ServiceUnavailable(reason: String)    extends EventSchedulingResult
  final case class SchedulingError(throwable: Throwable) extends EventSchedulingResult

  implicit def show[SE <: EventSchedulingResult]: Show[SE] = Show.show {
    case Accepted                   => "Accepted"
    case Busy                       => "Busy"
    case UnsupportedEventType       => "UnsupportedEventType"
    case BadRequest                 => "BadRequest"
    case ServiceUnavailable(reason) => s"ServiceUnavailable: $reason"
    case SchedulingError(_)         => "SchedulingError"
  }
}
