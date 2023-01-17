package io.renku.cli.model.generators

import io.renku.cli.model.{CliDatasetSameAs, DateModified}
import io.renku.generators.Generators
import io.renku.graph.model.datasets.PartResourceId
import io.renku.graph.model.{RenkuTinyTypeGenerators, datasets, generations}
import org.scalacheck.Gen

import java.time.Instant

trait BaseGenerators {
  def dateModifiedGen(min: Instant, max: Instant): Gen[DateModified] =
    Gen.choose(min, max).map(DateModified)

  val dateModified: Gen[DateModified] =
    dateModifiedGen(min = Instant.EPOCH, max = Instant.now())

  val datasetSameAs: Gen[CliDatasetSameAs] =
    RenkuTinyTypeGenerators.datasetSameAs.map(v => CliDatasetSameAs(v.value))

  val generationsResourceIdGen: Gen[generations.ResourceId] =
    Generators.noDashUuid.map(generations.ResourceId)

  val partResourceIdGen: Gen[datasets.PartResourceId] =
    Generators.noDashUuid.map(id => PartResourceId(id))
}

object BaseGenerators extends BaseGenerators
