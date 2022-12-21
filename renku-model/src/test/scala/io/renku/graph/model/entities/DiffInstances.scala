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

package io.renku.graph.model.entities

import cats.data.NonEmptyList
import com.softwaremill.diffx._
import io.renku.graph.model.datasets.{ExternalSameAs, InternalSameAs, TopmostSameAs}
import io.renku.graph.model.entities.Dataset.Provenance.ImportedInternalAncestorExternal
import io.renku.graph.model.entities.Dataset.{AdditionalInfo, Identification, Provenance}
import io.renku.graph.model.entityModel.{Location, LocationLike}
import io.renku.graph.model.images.{Image, ImageUri}
import io.renku.graph.model.{commandParameters, projects}
import io.renku.graph.model.projects.Visibility
import io.renku.tinytypes.{BooleanTinyType, InstantTinyType, IntTinyType, LocalDateTinyType, StringTinyType, UrlTinyType}

trait DiffInstances {

  implicit def stringTinyTypeDiff[A <: StringTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value)

  implicit def urlTinyTypeDiff[A <: UrlTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value)

  implicit def instantTinyTypeDiff[A <: InstantTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value.toString)

  implicit def localDateTinyTypeDiff[A <: LocalDateTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value.toString)

  implicit def intTinyTypeDiff[A <: IntTinyType]: Diff[A] =
    Diff.diffForNumeric[Int].contramap(_.value)

  implicit def boolTinyTypeDiff[A <: BooleanTinyType]: Diff[A] =
    Diff.diffForBoolean.contramap(_.value)

  implicit def nonEmptyListDiff[A: Diff]: Diff[NonEmptyList[A]] =
    Diff.diffForSeq[List, A].contramap(_.toList)

  implicit val personWithGitlabIdDiff: Diff[Person.WithGitLabId] = Diff.derived[Person.WithGitLabId]
  implicit val personWithEmailDiff:    Diff[Person.WithEmail]    = Diff.derived[Person.WithEmail]
  implicit val personWithNameDiff:     Diff[Person.WithNameOnly] = Diff.derived[Person.WithNameOnly]

  implicit val personDiff: Diff[Person] = Diff.derived[Person]

  implicit val explCommandParamDiff: Diff[StepPlanCommandParameter.ExplicitCommandParameter] =
    Diff.derived[StepPlanCommandParameter.ExplicitCommandParameter]

  implicit val implCommandParamDiff: Diff[StepPlanCommandParameter.ImplicitCommandParameter] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandParameter]

  implicit val commandInputDiff: Diff[StepPlanCommandParameter.CommandInput] =
    Diff.derived[StepPlanCommandParameter.CommandInput]
  implicit val commandOutputDiff: Diff[StepPlanCommandParameter.CommandOutput] =
    Diff.derived[StepPlanCommandParameter.CommandOutput]

  implicit lazy val locationCommandOutputDiff: Diff[StepPlanCommandParameter.LocationCommandOutput] =
    Diff.derived[StepPlanCommandParameter.LocationCommandOutput]

  implicit lazy val implicitCommandOutputDiff: Diff[StepPlanCommandParameter.ImplicitCommandOutput] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandOutput]

  implicit lazy val mappedCommandOutputDiff: Diff[StepPlanCommandParameter.MappedCommandOutput] =
    Diff.derived[StepPlanCommandParameter.MappedCommandOutput]

  implicit lazy val outputDefaultValueDiff: Diff[commandParameters.OutputDefaultValue] =
    Diff.diffForString.contramap(_.value.value)

  implicit val inputDefaultValueDiff: Diff[commandParameters.InputDefaultValue] =
    Diff.diffForString.contramap(_.value.value)

  implicit val ioStreamInDiff: Diff[commandParameters.IOStream.In] = Diff.derived[commandParameters.IOStream.In]

  implicit val ioStreamOutDiff: Diff[commandParameters.IOStream.Out] = Diff.derived[commandParameters.IOStream.Out]

  implicit val ioStreamStdInDiff: Diff[commandParameters.IOStream.StdIn] =
    Diff.derived[commandParameters.IOStream.StdIn]
  implicit val ioStreamStdOutDiff: Diff[commandParameters.IOStream.StdOut] =
    Diff.derived[commandParameters.IOStream.StdOut]
  implicit val ioStreamErrOutDiff: Diff[commandParameters.IOStream.StdErr] =
    Diff.derived[commandParameters.IOStream.StdErr]

  implicit val implicitCommandInputDiff: Diff[StepPlanCommandParameter.ImplicitCommandInput] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandInput]

  implicit val locationCommandInputDiff: Diff[StepPlanCommandParameter.LocationCommandInput] =
    Diff.derived[StepPlanCommandParameter.LocationCommandInput]

  implicit val commandParamDiff: Diff[StepPlanCommandParameter.CommandParameter] =
    Diff.derived[StepPlanCommandParameter.CommandParameter]

  implicit val planDerivationDiff: Diff[Plan.Derivation] =
    Diff.derived[Plan.Derivation]

  implicit val parameterMappingDiff: Diff[ParameterMapping] =
    Diff.derived[ParameterMapping]

  implicit val parameterLinkDiff: Diff[ParameterLink] =
    Diff.derived[ParameterLink]

  implicit val fileDiff: Diff[Location.File] =
    Diff.derived[Location.File]

  implicit val folderDiff: Diff[Location.Folder] =
    Diff.derived[Location.Folder]

  implicit val fileOrFolderDiff: Diff[Location.FileOrFolder] =
    Diff.derived[Location.FileOrFolder]

  implicit val locationLikeDiff: Diff[LocationLike] =
    Diff.derived[LocationLike]

  implicit val commandParameterValueDiff: Diff[ParameterValue.CommandParameterValue] =
    Diff.derived[ParameterValue.CommandParameterValue]

  implicit val commandInputValueDiff: Diff[ParameterValue.CommandInputValue] =
    Diff.derived[ParameterValue.CommandInputValue]

  implicit val commandOutputValue: Diff[ParameterValue.CommandOutputValue] =
    Diff.derived[ParameterValue.CommandOutputValue]

  implicit val locationParameterValueDiff: Diff[ParameterValue.LocationParameterValue] =
    Diff.derived[ParameterValue.LocationParameterValue]

  implicit val parameterValueDiff: Diff[ParameterValue] =
    Diff.derived[ParameterValue]

  implicit val stepPlanDiff: Diff[StepPlan] = Diff.derived[StepPlan]

  implicit val compositePlanDiff: Diff[CompositePlan] = Diff.derived[CompositePlan]

  implicit val planDiff: Diff[Plan] = Diff.derived[Plan]

  implicit val visibilityDiff: Diff[Visibility] =
    Diff.derived[Visibility]

  implicit val agentDiff: Diff[Agent] =
    Diff.derived[Agent]

  implicit val projectPathDiff: Diff[projects.Path] =
    Diff.diffForString.contramap(_.value)

  implicit val locationDiff: Diff[Location] =
    Diff.derived[Location]

  implicit val inputEntityDiff: Diff[Entity.InputEntity] =
    Diff.derived[Entity.InputEntity]

  implicit val outputEntityDiff: Diff[Entity.OutputEntity] =
    Diff.derived[Entity.OutputEntity]

  implicit val entityDiff: Diff[Entity] =
    Diff.derived[Entity]

  implicit val associationDiff: Diff[Association] =
    Diff.derived[Association]

  implicit val usagesDiff: Diff[Usage] =
    Diff.derived[Usage]

  implicit val generationDiff: Diff[Generation] =
    Diff.derived[Generation]

  implicit val activityDiff: Diff[Activity] =
    Diff.derived[Activity]

  implicit val externalSameAsDiff: Diff[ExternalSameAs] =
    Diff.derived[ExternalSameAs]

  implicit val internalSameAsDiff: Diff[InternalSameAs] =
    Diff.derived[InternalSameAs]

  implicit val importedInternalAncestorExternalDiff: Diff[ImportedInternalAncestorExternal] =
    Diff.derived[ImportedInternalAncestorExternal]

  implicit val provenanceDiff: Diff[Provenance] =
    Diff.derived[Provenance]

  implicit val identificationDiff: Diff[Identification] =
    Diff.derived[Identification]

  implicit val additionalInfoDiff: Diff[AdditionalInfo] =
    Diff.derived[AdditionalInfo]

  implicit val imageUriDiff: Diff[ImageUri] =
    Diff.diffForString.contramap(_.value)

  implicit val imageDiff: Diff[Image] =
    Diff.derived[Image]

  implicit val datasetPartDiff: Diff[DatasetPart] =
    Diff.derived[DatasetPart]

  implicit def datasetDiff: Diff[Dataset[Provenance]] =
    Diff.derived[Dataset[Provenance]]

  implicit val renkuProjectWithParentDiff: Diff[RenkuProject.WithParent] =
    Diff.derived[RenkuProject.WithParent]

  implicit val renkuProjectWithoutParentDiff: Diff[RenkuProject.WithoutParent] =
    Diff.derived[RenkuProject.WithoutParent]

  implicit val renkuProjectDiff: Diff[RenkuProject] =
    Diff.derived[RenkuProject]
}

object DiffInstances extends DiffInstances
