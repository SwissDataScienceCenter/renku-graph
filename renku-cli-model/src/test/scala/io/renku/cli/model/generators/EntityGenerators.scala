package io.renku.cli.model.generators

import io.renku.cli.model.{CliCollection, CliEntity}
import io.renku.generators.Generators
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

trait EntityGenerators {

  def entityGen: Gen[CliEntity] =
    for {
      id       <- RenkuTinyTypeGenerators.entityResourceIds
      location <- BaseGenerators.entityPathGen
      checksum <- RenkuTinyTypeGenerators.entityChecksums
      genIds   <- Generators.listOf(RenkuTinyTypeGenerators.generationsResourceIdGen)
    } yield CliEntity(id, location, checksum, genIds)

  def collectionMemberGen: Gen[CliCollection.Member] =
    Gen.frequency(
      1 -> collectionGen.map(CliCollection.Member.apply),
      9 -> entityGen.map(CliCollection.Member.apply)
    )

  def collectionGen: Gen[CliCollection] =
    for {
      id       <- RenkuTinyTypeGenerators.entityResourceIds
      location <- BaseGenerators.entityPathGen
      checksum <- RenkuTinyTypeGenerators.entityChecksums
      members  <- Generators.listOf(collectionMemberGen, max = 3)
    } yield CliCollection(id, location, checksum, members)

}

object EntityGenerators extends EntityGenerators
