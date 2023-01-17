package io.renku.cli.model.generators

import io.renku.cli.model.CliEntity
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

trait EntityGenerators {

  def entityGen(): Gen[CliEntity] =
    for {
      id          <- RenkuTinyTypeGenerators.entityResourceIds
      location    <- RenkuTinyTypeGenerators.entityLocations
      checksum    <- RenkuTinyTypeGenerators.entityChecksums
      generations <- Gen.choose(0, 5).flatMap(Gen.listOfN(_, BaseGenerators.generationsResourceIdGen))
    } yield CliEntity(id, location, checksum, generations)
}

object EntityGenerators extends EntityGenerators
