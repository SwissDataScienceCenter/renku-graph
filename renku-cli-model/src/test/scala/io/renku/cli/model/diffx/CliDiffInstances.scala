package io.renku.cli.model.diffx

import com.softwaremill.diffx.Diff
import io.renku.cli.model._
import io.renku.graph.model.diffx.ModelTinyTypesDiffInstances

trait CliDiffInstances extends ModelTinyTypesDiffInstances {

  implicit val cliPersonDiff: Diff[CliPerson] = Diff.derived[CliPerson]

  implicit val cliEntityDiff: Diff[CliEntity] = Diff.derived[CliEntity]

  implicit val cliDatasetFileDiff: Diff[CliDatasetFile] = Diff.derived[CliDatasetFile]

  implicit val cliDatasetDiff: Diff[CliDataset] = Diff.derived[CliDataset]

}

object CliDiffInstances extends CliDiffInstances
