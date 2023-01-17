package io.renku.cli.model.generators

import io.renku.cli.model.CliPerson
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.graph.model.persons.ResourceId
import org.scalacheck.Gen

trait PersonGenerators {

  def cliPersonIdGen(implicit renkuUrl: RenkuUrl): Gen[ResourceId] =
    Gen.oneOf(
      RenkuTinyTypeGenerators.personEmailResourceId,
      RenkuTinyTypeGenerators.personNameResourceId,
      RenkuTinyTypeGenerators.personOrcidResourceId
    )

  def cliPersonGen(implicit renkuUrl: RenkuUrl): Gen[CliPerson] =
    for {
      id    <- cliPersonIdGen
      name  <- RenkuTinyTypeGenerators.personNames
      email <- Gen.option(RenkuTinyTypeGenerators.personEmails)
      affil <- Gen.option(RenkuTinyTypeGenerators.personAffiliations)
    } yield CliPerson(id, name, email, affil)
}

object PersonGenerators extends PersonGenerators
