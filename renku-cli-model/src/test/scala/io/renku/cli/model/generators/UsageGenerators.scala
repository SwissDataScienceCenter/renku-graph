package io.renku.cli.model.generators

import io.renku.cli.model.CliUsage
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

trait UsageGenerators {

  def usageGen: Gen[CliUsage] =
    for {
      id <- RenkuTinyTypeGenerators.usageResourceIdGen
      entity <- EntityGenerators.entityGen
    } yield CliUsage(id, entity)
}

object UsageGenerators extends UsageGenerators
