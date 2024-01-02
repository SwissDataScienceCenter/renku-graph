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

package io.renku.graph.model.entities

import com.softwaremill.diffx._
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.graph.model.entities.Dataset.Provenance.ImportedInternalAncestorExternal
import io.renku.graph.model.entities.Dataset.{AdditionalInfo, Identification, Provenance}

trait DiffInstances extends CliDiffInstances {

  implicit val personWithGitlabIdDiff: Diff[Person.WithGitLabId] = Diff.derived[Person.WithGitLabId]
  implicit val personWithEmailDiff:    Diff[Person.WithEmail]    = Diff.derived[Person.WithEmail]
  implicit val personWithNameDiff:     Diff[Person.WithNameOnly] = Diff.derived[Person.WithNameOnly]

  implicit val personDiff: Diff[Person] = Diff.derived[Person]

  implicit val explicitCommandParamDiff: Diff[StepPlanCommandParameter.ExplicitCommandParameter] =
    Diff.derived[StepPlanCommandParameter.ExplicitCommandParameter]
  implicit val implicitCommandParamDiff: Diff[StepPlanCommandParameter.ImplicitCommandParameter] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandParameter]

  implicit val commandInputDiff: Diff[StepPlanCommandParameter.CommandInput] =
    Diff.derived[StepPlanCommandParameter.CommandInput]
  implicit val commandOutputDiff: Diff[StepPlanCommandParameter.CommandOutput] =
    Diff.derived[StepPlanCommandParameter.CommandOutput]
  implicit lazy val commandInputOrOutputDiff: Diff[StepPlanCommandParameter.CommandInputOrOutput] =
    Diff.derived[StepPlanCommandParameter.CommandInputOrOutput]

  implicit lazy val locationCommandOutputDiff: Diff[StepPlanCommandParameter.LocationCommandOutput] =
    Diff.derived[StepPlanCommandParameter.LocationCommandOutput]
  implicit lazy val mappedCommandOutputDiff: Diff[StepPlanCommandParameter.MappedCommandOutput] =
    Diff.derived[StepPlanCommandParameter.MappedCommandOutput]
  implicit lazy val implicitCommandOutputDiff: Diff[StepPlanCommandParameter.ImplicitCommandOutput] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandOutput]

  implicit val locationCommandInputDiff: Diff[StepPlanCommandParameter.LocationCommandInput] =
    Diff.derived[StepPlanCommandParameter.LocationCommandInput]
  implicit lazy val mappedCommandInputDiff: Diff[StepPlanCommandParameter.MappedCommandInput] =
    Diff.derived[StepPlanCommandParameter.MappedCommandInput]
  implicit val implicitCommandInputDiff: Diff[StepPlanCommandParameter.ImplicitCommandInput] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandInput]

  implicit val commandParamDiff: Diff[StepPlanCommandParameter.CommandParameter] =
    Diff.derived[StepPlanCommandParameter.CommandParameter]

  implicit val planDerivationDiff: Diff[Plan.Derivation] = Diff.derived[Plan.Derivation]

  implicit val parameterMappingDiff: Diff[ParameterMapping] = Diff.derived[ParameterMapping]

  implicit val parameterLinkDiff: Diff[ParameterLink] = Diff.derived[ParameterLink]

  implicit val commandParameterValueDiff: Diff[ParameterValue.CommandParameterValue] =
    Diff.derived[ParameterValue.CommandParameterValue]
  implicit val commandInputValueDiff: Diff[ParameterValue.CommandInputValue] =
    Diff.derived[ParameterValue.CommandInputValue]
  implicit val commandOutputValue: Diff[ParameterValue.CommandOutputValue] =
    Diff.derived[ParameterValue.CommandOutputValue]
  implicit val locationParameterValueDiff: Diff[ParameterValue.LocationParameterValue] =
    Diff.derived[ParameterValue.LocationParameterValue]
  implicit val parameterValueDiff: Diff[ParameterValue] = Diff.derived[ParameterValue]

  implicit val nonModifiedStepPlanDiff: Diff[StepPlan.NonModified] = Diff.derived[StepPlan.NonModified]
  implicit val modifiedStepPlanDiff:    Diff[StepPlan.Modified]    = Diff.derived[StepPlan.Modified]
  implicit val stepPlanDiff:            Diff[StepPlan]             = Diff.derived[StepPlan]

  implicit val nonModifiedCompositePlanDiff: Diff[CompositePlan.NonModified] = Diff.derived[CompositePlan.NonModified]
  implicit val modifiedCompositePlanDiff:    Diff[CompositePlan.Modified]    = Diff.derived[CompositePlan.Modified]
  implicit val compositePlanDiff:            Diff[CompositePlan]             = Diff.derived[CompositePlan]

  implicit val planDiff: Diff[Plan] = Diff.derived[Plan]

  implicit val agentDiff: Diff[Agent] = Diff.derived[Agent]

  implicit val inputEntityDiff:  Diff[Entity.InputEntity]  = Diff.derived[Entity.InputEntity]
  implicit val outputEntityDiff: Diff[Entity.OutputEntity] = Diff.derived[Entity.OutputEntity]
  implicit val entityDiff:       Diff[Entity]              = Diff.derived[Entity]

  implicit val renkuAgentAssociationDiff:  Diff[Association.WithRenkuAgent]  = Diff.derived[Association.WithRenkuAgent]
  implicit val personAgentAssociationDiff: Diff[Association.WithPersonAgent] = Diff.derived[Association.WithPersonAgent]
  implicit val associationDiff:            Diff[Association]                 = Diff.derived[Association]

  implicit val usagesDiff: Diff[Usage] = Diff.derived[Usage]

  implicit val generationDiff: Diff[Generation] = Diff.derived[Generation]

  implicit val activityDiff: Diff[Activity] = Diff.derived[Activity]

  implicit val importedInternalAncestorExternalDiff: Diff[ImportedInternalAncestorExternal] =
    Diff.derived[ImportedInternalAncestorExternal]

  implicit val provenanceDiff: Diff[Provenance] = Diff.derived[Provenance]

  implicit val identificationDiff: Diff[Identification] = Diff.derived[Identification]

  implicit val additionalInfoDiff: Diff[AdditionalInfo] = Diff.derived[AdditionalInfo]

  implicit val datasetPartDiff: Diff[DatasetPart] = Diff.derived[DatasetPart]

  implicit val publicationEventDiff: Diff[PublicationEvent] = Diff.derived[PublicationEvent]

  implicit def datasetDiff: Diff[Dataset[Provenance]] = Diff.derived[Dataset[Provenance]]

  implicit val projectMemberDiff: Diff[Project.Member] =
    Diff.derived[Project.Member]

  implicit val renkuProjectWithParentDiff: Diff[RenkuProject.WithParent] =
    Diff.derived[RenkuProject.WithParent]

  implicit val renkuProjectWithoutParentDiff: Diff[RenkuProject.WithoutParent] =
    Diff.derived[RenkuProject.WithoutParent]

  implicit val renkuProjectDiff: Diff[RenkuProject] = Diff.derived[RenkuProject]

  implicit val nonRenkuProjectWithoutParentDiff: Diff[NonRenkuProject.WithoutParent] =
    Diff.derived[NonRenkuProject.WithoutParent]

  implicit val nonRenkuProjectWithParentDiff: Diff[NonRenkuProject.WithParent] =
    Diff.derived[NonRenkuProject.WithParent]

  implicit val nonRenkuProjectDiff: Diff[NonRenkuProject] = Diff.derived[NonRenkuProject]

  implicit val projectDiff: Diff[Project] = Diff.derived[Project]
}

object DiffInstances extends DiffInstances
