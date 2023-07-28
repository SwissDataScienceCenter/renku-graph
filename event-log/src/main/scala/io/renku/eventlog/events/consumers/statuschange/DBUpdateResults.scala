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

package io.renku.eventlog.events.consumers.statuschange

import cats.Monoid
import cats.syntax.all._
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.projects

private sealed trait DBUpdateResults {
  lazy val widen: DBUpdateResults = this
}

private object DBUpdateResults {

  final case class ForProjects(statusCounts: Set[(projects.Slug, Map[EventStatus, Int])]) extends DBUpdateResults {
    def apply(project: projects.Slug): Map[EventStatus, Int] =
      statusCounts.find(_._1 == project).map(_._2).getOrElse(Map.empty)
  }

  object ForProjects {

    lazy val empty: ForProjects = ForProjects(Set.empty)

    def apply(projectSlug: projects.Slug, statusCount: Map[EventStatus, Int]): ForProjects =
      ForProjects(Set(projectSlug -> statusCount))
  }

  final case object ForAllProjects extends DBUpdateResults

  implicit val monoid: Monoid[DBUpdateResults.ForProjects] = new Monoid[DBUpdateResults.ForProjects] {
    override def combine(x: DBUpdateResults.ForProjects, y: DBUpdateResults.ForProjects): DBUpdateResults.ForProjects =
      ForProjects(
        (x.statusCounts.toSeq ++ y.statusCounts.toSeq)
          .groupBy(_._1)
          .map { case (slug, slugAndCounts) => slug -> slugAndCounts.map(_._2).combineAll }
          .toSet
      )

    override def empty: ForProjects = ForProjects.empty
  }
}
