/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._

final case class Project(id: projects.GitLabId, slug: projects.Slug)

object Project {
  implicit lazy val show: Show[Project] = Show.show { case Project(id, slug) =>
    s"projectId = $id, projectSlug = $slug"
  }

  implicit val jsonDecoder: Decoder[Project] = deriveDecoder[Project]
  implicit val jsonEncoder: Encoder[Project] = deriveEncoder[Project]
}

sealed trait EventSchedulingResult extends Product with Serializable {
  def widen: EventSchedulingResult = this
}

object EventSchedulingResult {
  type Accepted = Accepted.type
  case object Accepted                                   extends EventSchedulingResult
  case object Busy                                       extends EventSchedulingResult
  case object UnsupportedEventType                       extends EventSchedulingResult
  final case class BadRequest(reason: String)            extends EventSchedulingResult
  final case class ServiceUnavailable(reason: String)    extends EventSchedulingResult
  final case class SchedulingError(throwable: Throwable) extends EventSchedulingResult

  implicit def show[SR <: EventSchedulingResult]: Show[SR] = Show.show {
    case Accepted                   => "Accepted"
    case Busy                       => "Busy"
    case UnsupportedEventType       => "UnsupportedEventType"
    case BadRequest(reason)         => s"BadRequest: $reason"
    case ServiceUnavailable(reason) => s"ServiceUnavailable: $reason"
    case SchedulingError(ex)        => s"SchedulingError: ${ex.getMessage}"
  }
}
