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

package io.renku.cli.model.diffx

import com.softwaremill.diffx.Diff
import io.renku.cli.model._
import io.renku.graph.model.diffx.ModelTinyTypesDiffInstances

trait CliDiffInstances extends ModelTinyTypesDiffInstances {

  implicit lazy val cliPersonDiff: Diff[CliPerson] = Diff.derived[CliPerson]

  implicit lazy val cliSingleEntityDiff: Diff[CliSingleEntity] = Diff.derived[CliSingleEntity]

  implicit lazy val cliCollectionEntityDiff: Diff[CliCollectionEntity] = Diff.derived[CliCollectionEntity]

  implicit lazy val cliEntityDiff: Diff[CliEntity] = Diff.derived[CliEntity]

  implicit lazy val cliDatasetFileDiff: Diff[CliDatasetFile] = Diff.derived[CliDatasetFile]

  implicit lazy val cliProvenanceDiff: Diff[CliDatasetProvenance] = Diff.derived[CliDatasetProvenance]

  implicit lazy val cliDatasetDiff: Diff[CliDataset] = Diff.derived[CliDataset]

  implicit lazy val cliPublicationEventDiff: Diff[CliPublicationEvent] = Diff.derived[CliPublicationEvent]

  implicit lazy val cliSoftwareAgentDiff: Diff[CliSoftwareAgent] = Diff.derived[CliSoftwareAgent]

  implicit lazy val cliParameterValueDiff: Diff[CliParameterValue] = Diff.derived[CliParameterValue]

  implicit lazy val cliCommandParameterDiff: Diff[CliCommandParameter] = Diff.derived[CliCommandParameter]

  implicit lazy val cliStreamTypeDiff: Diff[CliMappedIOStream.StreamType] = Diff.derived[CliMappedIOStream.StreamType]

  implicit lazy val cliMappedIOStreamDiff: Diff[CliMappedIOStream] = Diff.derived[CliMappedIOStream]

  implicit lazy val cliCommandInputDiff: Diff[CliCommandInput] = Diff.derived[CliCommandInput]

  implicit lazy val cliCommandOutputDiff: Diff[CliCommandOutput] = Diff.derived[CliCommandOutput]

  implicit lazy val cliMappedParamDiff: Diff[CliParameterMapping.MappedParam] =
    Diff.derived[CliParameterMapping.MappedParam]

  implicit lazy val cliParameterMappingDiff: Diff[CliParameterMapping] = Diff.derived[CliParameterMapping]

  implicit lazy val cliParameterLinkSinkDiff: Diff[CliParameterLink.Sink] = Diff.derived[CliParameterLink.Sink]

  implicit lazy val cliParameterLinkDiff: Diff[CliParameterLink] = Diff.derived[CliParameterLink]

  implicit lazy val cliPlanDiff: Diff[CliPlan] = Diff.derived[CliPlan]

  implicit lazy val cliCompositePlanChildPlanDiff: Diff[CliCompositePlan.ChildPlan] =
    Diff.derived[CliCompositePlan.ChildPlan]

  implicit lazy val cliCompositePlan: Diff[CliCompositePlan] = Diff.derived[CliCompositePlan]

  implicit lazy val cliWorkflowFilePlanDiff: Diff[CliWorkflowFilePlan] = Diff.derived[CliWorkflowFilePlan]

  implicit lazy val cliWorkflowFileCompositePlanDiff: Diff[CliWorkflowFileCompositePlan] =
    Diff.derived[CliWorkflowFileCompositePlan]

  implicit lazy val cliAssociatedPlanDiff: Diff[CliAssociation.AssociatedPlan] =
    Diff.derived[CliAssociation.AssociatedPlan]

  implicit lazy val cliAssociationDiff: Diff[CliAssociation] = Diff.derived[CliAssociation]

  implicit lazy val cliAgentSoftwareDiff: Diff[CliAgent.Software] = Diff.derived[CliAgent.Software]

  implicit lazy val cliAgentPersonDiff: Diff[CliAgent.Person] = Diff.derived[CliAgent.Person]

  implicit lazy val cliAgentDiff: Diff[CliAgent] = Diff.derived[CliAgent]

  implicit lazy val cliUsageDiff: Diff[CliUsage] = Diff.derived[CliUsage]

  implicit lazy val cliGenerationDiff: Diff[CliGeneration] = Diff.derived[CliGeneration]

  implicit lazy val cliActivityDiff: Diff[CliActivity] = Diff.derived[CliActivity]

  implicit lazy val cliProjectPlanDiff: Diff[CliProject.ProjectPlan] = Diff.derived[CliProject.ProjectPlan]

  implicit lazy val cliProjectDiff: Diff[CliProject] = Diff.derived[CliProject]
}

object CliDiffInstances extends CliDiffInstances
