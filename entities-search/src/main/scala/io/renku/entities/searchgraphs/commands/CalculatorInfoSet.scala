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

package io.renku.entities.searchgraphs
package commands

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.entities.Project

private final case class CalculatorInfoSet(project:        Project,
                                           maybeModelInfo: Option[SearchInfo],
                                           maybeTSInfo:    Option[SearchInfo]
)

private object CalculatorInfoSet {

  implicit val show: Show[CalculatorInfoSet] = Show.show {
    case CalculatorInfoSet(project, maybeModelInfo, maybeTSInfo) =>
      def toString(info: SearchInfo) = List(
        show"topmostSameAs = ${info.topmostSameAs}",
        show"name = ${info.name}",
        show"visibility = ${info.visibility}",
        show"links = [${info.links.map(link => show"projectId = ${link.projectId}, datasetId = ${link.datasetId}").intercalate("; ")}}]"
      ).mkString(", ")

      show"projectId = ${project.resourceId}, projectPath = ${project.path}" +
        maybeModelInfo.map(i => show", modelInfo = [${toString(i)}]").getOrElse("") +
        maybeTSInfo.map(i => show", tsInfo = [${toString(i)}]").getOrElse("")
  }
}
