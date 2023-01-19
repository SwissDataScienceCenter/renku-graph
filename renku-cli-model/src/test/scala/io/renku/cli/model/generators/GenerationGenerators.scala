package io.renku.cli.model.generators

import io.renku.cli.model.CliGeneration
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

trait GenerationGenerators {

  def generationEntityGen: Gen[CliGeneration.QualifiedGeneration] =
    Gen.oneOf(
      EntityGenerators.entityGen.map(CliGeneration.QualifiedGeneration.apply),
      EntityGenerators.collectionGen.map(CliGeneration.QualifiedGeneration.apply)
    )

  def generationGen: Gen[CliGeneration] =
    for {
      id     <- RenkuTinyTypeGenerators.generationsResourceIdGen
      entity <- generationEntityGen
    } yield CliGeneration(id, entity)
}

object GenerationGenerators extends GenerationGenerators
