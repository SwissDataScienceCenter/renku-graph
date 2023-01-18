package io.renku.cli.model.generators

import io.renku.cli.model.{CliDatasetSameAs, DateModified}
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

import java.time.Instant

trait BaseGenerators {
  def dateModifiedGen(min: Instant, max: Instant): Gen[DateModified] =
    Gen.choose(min, max).map(DateModified)

  val dateModified: Gen[DateModified] =
    dateModifiedGen(min = Instant.EPOCH, max = Instant.now())

  val datasetSameAs: Gen[CliDatasetSameAs] =
    RenkuTinyTypeGenerators.datasetSameAs.map(v => CliDatasetSameAs(v.value))
}

object BaseGenerators extends BaseGenerators
