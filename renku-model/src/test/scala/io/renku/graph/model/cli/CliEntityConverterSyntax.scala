package io.renku.graph.model.cli

import io.renku.graph.model.entities

/** Convert from the production entities into cli entities. The test model entities can
 * be converted via its implicit `.to[CliType]`.
 */
trait CliEntityConverterSyntax {

  final implicit class DatasetOps(self: entities.Dataset[entities.Dataset.Provenance]) {
    def toCliEntity: CliDataset =
      CliConv.from(self)
  }

}

object CliEntityConverterSyntax extends CliEntityConverterSyntax
