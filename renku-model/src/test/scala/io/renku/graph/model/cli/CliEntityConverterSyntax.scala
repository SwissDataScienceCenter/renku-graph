package io.renku.graph.model.cli

import io.renku.graph.model.entities

trait CliEntityConverterSyntax {

  final implicit class DatasetOps(self: entities.Dataset[entities.Dataset.Provenance]) {
    def toCliEntity: CliDataset =
      CliConv.from(self)
  }

}

object CliEntityConverterSyntax extends CliEntityConverterSyntax
