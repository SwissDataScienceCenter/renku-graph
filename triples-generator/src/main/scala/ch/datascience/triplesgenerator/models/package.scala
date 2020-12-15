package ch.datascience.triplesgenerator

import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.projects.SchemaVersion

package object models {
  final case class RenkuVersionPair(cliVersion: CliVersion, schemaVersion: SchemaVersion)
      extends Product
      with Serializable

}
