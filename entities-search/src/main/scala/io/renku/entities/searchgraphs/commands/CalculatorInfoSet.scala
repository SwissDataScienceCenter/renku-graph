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
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.graph.model.projects

private sealed trait CalculatorInfoSet {
  val project: ProjectIdentification
}

private object CalculatorInfoSet {

  final case class ModelInfoOnly(project: ProjectIdentification, modelInfo: SearchInfo) extends CalculatorInfoSet

  final case class TSInfoOnly(project:        ProjectIdentification,
                              tsInfo:         SearchInfo,
                              tsVisibilities: Map[projects.ResourceId, projects.Visibility]
  ) extends CalculatorInfoSet

  final case class AllInfos(project:        ProjectIdentification,
                            modelInfo:      SearchInfo,
                            tsInfo:         SearchInfo,
                            tsVisibilities: Map[projects.ResourceId, projects.Visibility]
  ) extends CalculatorInfoSet

  def from(project:        ProjectIdentification,
           maybeModelInfo: Option[SearchInfo],
           maybeTSInfo:    Option[SearchInfo],
           tsVisibilities: Map[projects.ResourceId, projects.Visibility]
  ): Either[Exception, CalculatorInfoSet] =
    validate(project, maybeModelInfo, maybeTSInfo) >>= { _ =>
      instantiate(project, maybeModelInfo, maybeTSInfo, tsVisibilities)
    }

  private def validate(project:        ProjectIdentification,
                       maybeModelInfo: Option[SearchInfo],
                       maybeTSInfo:    Option[SearchInfo]
  ): Either[Exception, Unit] =
    (maybeModelInfo, maybeTSInfo) match {
      case (Some(modelInfo), _) if modelInfo.links.size > 1 =>
        new Exception(show"CalculatorInfoSet for ${project.resourceId} is linked to many projects").asLeft
      case (Some(modelInfo), _) if modelInfo.links.head.projectId != project.resourceId =>
        new Exception(
          show"CalculatorInfoSet for ${project.resourceId} has model linked to ${modelInfo.links.head.projectId}"
        ).asLeft
      case _ => ().asRight
    }

  private lazy val instantiate: (ProjectIdentification,
                                 Option[SearchInfo],
                                 Option[SearchInfo],
                                 Map[projects.ResourceId, projects.Visibility]
  ) => Either[Exception, CalculatorInfoSet] = {
    case (project, Some(modelInfo), None, _)             => ModelInfoOnly(project, modelInfo).asRight
    case (project, None, Some(tsInfo), tsVisibilities)   => TSInfoOnly(project, tsInfo, tsVisibilities).asRight
    case (project, Some(modelInfo), Some(tsInfo), tsVis) => AllInfos(project, modelInfo, tsInfo, tsVis).asRight
    case (project, None, None, _) =>
      new Exception(show"CalculatorInfoSet for ${project.resourceId} has no infos").asLeft
  }

  implicit val show: Show[CalculatorInfoSet] = Show.show {
    case CalculatorInfoSet.ModelInfoOnly(project, modelInfo) =>
      show"$project, modelInfo = [${toString(modelInfo)}]"
    case CalculatorInfoSet.TSInfoOnly(project, tsInfo, _) =>
      show"$project, tsInfo = [${toString(tsInfo)}]"
    case CalculatorInfoSet.AllInfos(project, modelInfo, tsInfo, _) =>
      show"$project, modelInfo = [${toString(modelInfo)}], tsInfo = [${toString(tsInfo)}]"
  }

  private def toString(info: SearchInfo) = List(
    show"topmostSameAs = ${info.topmostSameAs}",
    show"name = ${info.name}",
    show"visibility = ${info.visibility}",
    show"links = [${info.links.map(link => show"projectId = ${link.projectId}, datasetId = ${link.datasetId}").intercalate("; ")}}]"
  ).mkString(", ")

}
