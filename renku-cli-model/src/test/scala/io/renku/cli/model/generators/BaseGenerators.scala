package io.renku.cli.model.generators

import io.renku.cli.model.{CliDatasetSameAs, DateModified, EntityPath}
import io.renku.generators.Generators
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

import java.time.Instant

trait BaseGenerators {
  val entityPathGen: Gen[EntityPath] =
    Generators.relativePaths().map(EntityPath)

  def dateModifiedGen(min: Instant, max: Instant): Gen[DateModified] =
    Gen.choose(min, max).map(DateModified)

  val dateModified: Gen[DateModified] =
    dateModifiedGen(min = Instant.EPOCH, max = Instant.now())

  val datasetSameAs: Gen[CliDatasetSameAs] =
    RenkuTinyTypeGenerators.datasetSameAs.map(v => CliDatasetSameAs(v.value))
}

object BaseGenerators extends BaseGenerators
