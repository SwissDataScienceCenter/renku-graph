package io.renku.cli.model.generators

import io.renku.cli.model.CliParameterValue
import io.renku.generators.Generators
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

trait ParameterValueGenerators {

  def parameterValueGen: Gen[CliParameterValue] =
    for {
      id    <- RenkuTinyTypeGenerators.parameterValueIdGen
      ref   <- RenkuTinyTypeGenerators.commandParameterResourceId
      value <- Generators.nonEmptyStrings()
    } yield CliParameterValue(id, ref, value)
}

object ParameterValueGenerators extends ParameterValueGenerators
