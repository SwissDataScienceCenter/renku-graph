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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.projects

private[cleanuprequest] trait CleanUpRequestEvent extends Product with Serializable

private[cleanuprequest] object CleanUpRequestEvent {
  def apply(projectId: projects.GitLabId, projectSlug: projects.Slug): CleanUpRequestEvent =
    CleanUpRequestEvent.Full(projectId, projectSlug)
  def apply(projectSlug: projects.Slug): CleanUpRequestEvent =
    CleanUpRequestEvent.Partial(projectSlug)

  final case class Full(projectId: projects.GitLabId, projectSlug: projects.Slug) extends CleanUpRequestEvent
  final case class Partial(projectSlug: projects.Slug)                            extends CleanUpRequestEvent

  implicit val show: Show[CleanUpRequestEvent] = Show.show {
    case e: Full    => e.show
    case e: Partial => e.show
  }

  implicit val showFull: Show[Full] = Show.show { case Full(id, slug) =>
    show"projectId = $id, projectSlug = $slug"
  }
  implicit val showPartial: Show[Partial] = Show.show { case Partial(slug) => show"projectSlug = $slug" }
}
