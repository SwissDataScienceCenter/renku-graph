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

package io.renku.entities.searchgraphs.datasets
package commands

import SearchInfoLens.searchInfoLinks
import cats.Show
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.ProjectIdentification

private sealed trait CalculatorInfoSet {
  val project:       ProjectIdentification
  val topmostSameAs: datasets.TopmostSameAs
  def asDatasetSearchInfo: Option[DatasetSearchInfo]
}

private object CalculatorInfoSet {

  final case class ModelInfoOnly(project: ProjectIdentification, modelInfo: ModelDatasetSearchInfo)
      extends CalculatorInfoSet {
    override val topmostSameAs:            datasets.TopmostSameAs    = modelInfo.topmostSameAs
    override lazy val asDatasetSearchInfo: Option[DatasetSearchInfo] = modelInfo.toDatasetSearchInfo.some
  }

  final case class TSInfoOnly(project: ProjectIdentification, tsInfo: TSDatasetSearchInfo) extends CalculatorInfoSet {
    override val topmostSameAs:       datasets.TopmostSameAs    = tsInfo.topmostSameAs
    override val asDatasetSearchInfo: Option[DatasetSearchInfo] = None
  }

  final case class BothInfos(project:   ProjectIdentification,
                             modelInfo: ModelDatasetSearchInfo,
                             tsInfo:    TSDatasetSearchInfo
  ) extends CalculatorInfoSet {
    override val topmostSameAs: datasets.TopmostSameAs = modelInfo.topmostSameAs
    override lazy val asDatasetSearchInfo: Option[DatasetSearchInfo] =
      modelInfo.toDatasetSearchInfo.some
        .map(
          searchInfoLinks.replace {
            NonEmptyList.one(modelInfo.link).prependList(tsInfo.links.filterNot(_.projectId == project.resourceId))
          }
        )
  }

  def from(project:        ProjectIdentification,
           maybeModelInfo: Option[ModelDatasetSearchInfo],
           maybeTSInfo:    Option[TSDatasetSearchInfo]
  ): Either[Exception, CalculatorInfoSet] =
    validate(project, maybeModelInfo, maybeTSInfo) >>
      instantiate(project, maybeModelInfo, maybeTSInfo)

  private def validate(project:        ProjectIdentification,
                       maybeModelInfo: Option[ModelDatasetSearchInfo],
                       maybeTSInfo:    Option[TSDatasetSearchInfo]
  ): Either[Exception, Unit] =
    (validateTopSameAs(project)(maybeModelInfo -> maybeTSInfo) |+|
      validateModelProject(project)(maybeModelInfo)).toEither.leftMap(errs => new Exception(errs.mkString_("; ")))

  private def validateTopSameAs(
      project: ProjectIdentification
  ): ((Option[ModelDatasetSearchInfo], Option[TSDatasetSearchInfo])) => ValidatedNel[String, Unit] = {
    case (Some(modelInfo), Some(tsInfo)) if modelInfo.topmostSameAs != tsInfo.topmostSameAs =>
      Validated.invalidNel(show"CalculatorInfoSet for ${project.resourceId} has different TopmostSameAs")
    case _ => Validated.validNel(())
  }

  private def validateModelProject(
      project: ProjectIdentification
  ): Option[ModelDatasetSearchInfo] => ValidatedNel[String, Unit] = {
    case Some(modelInfo) if modelInfo.link.projectId != project.resourceId =>
      Validated.invalidNel(show"CalculatorInfoSet for ${project.resourceId} has link to ${modelInfo.link.projectId}")
    case _ => Validated.validNel(())
  }

  private lazy val instantiate: (ProjectIdentification,
                                 Option[ModelDatasetSearchInfo],
                                 Option[TSDatasetSearchInfo]
  ) => Either[Exception, CalculatorInfoSet] = {
    case (project, Some(modelInfo), None)         => ModelInfoOnly(project, modelInfo).asRight
    case (project, None, Some(tsInfo))            => TSInfoOnly(project, tsInfo).asRight
    case (project, Some(modelInfo), Some(tsInfo)) => BothInfos(project, modelInfo, tsInfo).asRight
    case (project, None, None) => new Exception(show"CalculatorInfoSet for ${project.resourceId} has no infos").asLeft
  }

  private implicit class ModelInfoOps(modelInfo: ModelDatasetSearchInfo) {
    lazy val toDatasetSearchInfo: DatasetSearchInfo = DatasetSearchInfo(
      modelInfo.topmostSameAs,
      modelInfo.name,
      modelInfo.slug,
      modelInfo.createdOrPublished,
      modelInfo.maybeDateModified,
      modelInfo.creators,
      modelInfo.keywords,
      modelInfo.maybeDescription,
      modelInfo.images,
      NonEmptyList.one(modelInfo.link)
    )
  }

  implicit val show: Show[CalculatorInfoSet] = Show.show {
    case CalculatorInfoSet.ModelInfoOnly(project, modelInfo) =>
      show"$project, modelInfo = [${toString(modelInfo)}]"
    case CalculatorInfoSet.TSInfoOnly(project, tsInfo) =>
      show"$project, tsInfo = [$tsInfo]"
    case CalculatorInfoSet.BothInfos(project, modelInfo, tsInfo) =>
      show"$project, modelInfo = [${toString(modelInfo)}], tsInfo = [$tsInfo]"
  }

  private def toString(info: ModelDatasetSearchInfo) = List(
    show"topmostSameAs = ${info.topmostSameAs}",
    show"name = ${info.name}",
    show"slug = ${info.slug}",
    show"visibility = ${info.link.visibility}",
    show"link = ${info.link}"
  ).mkString(", ")
}
