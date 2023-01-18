package io.renku.cli.model.diffx

import com.softwaremill.diffx.Diff
import io.renku.cli.model._
import io.renku.graph.model.diffx.ModelTinyTypesDiffInstances

trait CliDiffInstances extends ModelTinyTypesDiffInstances {

  implicit val cliPersonDiff: Diff[CliPerson] = Diff.derived[CliPerson]

  implicit val cliEntityDiff: Diff[CliEntity] = Diff.derived[CliEntity]

  implicit val cliDatasetFileDiff: Diff[CliDatasetFile] = Diff.derived[CliDatasetFile]

  implicit val cliDatasetDiff: Diff[CliDataset] = Diff.derived[CliDataset]

  implicit val cliAgentDiff: Diff[CliAgent] = Diff.derived[CliAgent]

  implicit val cliParameterValueDiff: Diff[CliParameterValue] = Diff.derived[CliParameterValue]

  implicit val cliCommandParameterDiff: Diff[CliCommandParameter] = Diff.derived[CliCommandParameter]

  implicit val cliStreamTypeDiff: Diff[CliMappedIOStream.StreamType] = Diff.derived[CliMappedIOStream.StreamType]

  implicit val cliMappedIOStreamDiff: Diff[CliMappedIOStream] = Diff.derived[CliMappedIOStream]

  implicit val cliCommandInputDiff: Diff[CliCommandInput] = Diff.derived[CliCommandInput]

  implicit val cliCommandOutputDiff: Diff[CliCommandOutput] = Diff.derived[CliCommandOutput]

  implicit val cliMappedParamDiff: Diff[CliParameterMapping.MappedParam] = Diff.derived[CliParameterMapping.MappedParam]

  implicit val cliParameterMappingDiff: Diff[CliParameterMapping] = Diff.derived[CliParameterMapping]

  implicit val cliParameterLinkSinkDiff: Diff[CliParameterLink.Sink] = Diff.derived[CliParameterLink.Sink]

  implicit val cliParameterLinkDiff: Diff[CliParameterLink] = Diff.derived[CliParameterLink]

  implicit val cliPlanDiff: Diff[CliPlan] = Diff.derived[CliPlan]

  implicit val cliCompositePlanChildPlanDiff: Diff[CliCompositePlan.ChildPlan] =
    Diff.derived[CliCompositePlan.ChildPlan]

  implicit val cliCompositePlan: Diff[CliCompositePlan] = Diff.derived[CliCompositePlan]

  implicit val cliWorkflowFilePlanDiff: Diff[CliWorkflowFilePlan] = Diff.derived[CliWorkflowFilePlan]

  implicit val cliWorkflowFileCompositePlanDiff: Diff[CliWorkflowFileCompositePlan] =
    Diff.derived[CliWorkflowFileCompositePlan]
}

object CliDiffInstances extends CliDiffInstances
