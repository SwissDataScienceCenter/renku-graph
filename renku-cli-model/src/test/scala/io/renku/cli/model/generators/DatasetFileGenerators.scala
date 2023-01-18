package io.renku.cli.model.generators

import io.renku.cli.model.CliDatasetFile
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

import java.time.Instant

trait DatasetFileGenerators {
  def datasetFileGen(minCreated: Instant): Gen[CliDatasetFile] =
    for {
      id          <- RenkuTinyTypeGenerators.partResourceIdGen
      external    <- RenkuTinyTypeGenerators.datasetPartExternals
      entity      <- EntityGenerators.entityGen()
      created     <- RenkuTinyTypeGenerators.datasetCreatedDates(minCreated)
      source      <- Gen.option(RenkuTinyTypeGenerators.datasetPartSources)
      invalidTime <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(created))
    } yield CliDatasetFile(id, external, entity, created, source, invalidTime)
}

object DatasetFileGenerators extends DatasetFileGenerators
