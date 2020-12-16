package ch.datascience.triplesgenerator.generators

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.triplesgenerator.models.RenkuVersionPair
import org.scalacheck.Gen

object VersionGenerators {

  implicit val renkuVersionPairs: Gen[RenkuVersionPair] = for {
    cliVersion    <- cliVersions
    schemaVersion <- projectSchemaVersions
  } yield RenkuVersionPair(cliVersion, schemaVersion)

}
