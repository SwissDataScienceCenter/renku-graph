package io.renku.graph.model.cli

import io.renku.cli.model.{CliDataset, CliEntity, CliPerson, CliPublicationEvent}
import io.renku.graph.model.entities

/** Convert from the production entities into cli entities. The test model entities can
 * be converted via its implicit `.to[CliType]`.
 */
trait CliEntityConverterSyntax {

  final implicit class PersonOps(self: entities.Person) {
    def toCliEntity: CliPerson = CliConverters.from(self)
  }

  final implicit class EntityOps(self: entities.Entity) {
    def toCliEntity: CliEntity = CliConverters.from(self)
  }

  final implicit class DatasetOps(self: entities.Dataset[entities.Dataset.Provenance]) {
    def toCliEntity: CliDataset = CliConverters.from(self)
  }

  final implicit class PublicationEventOps(self: entities.PublicationEvent) {
    def toCliEntity: CliPublicationEvent = CliConverters.from(self)
  }
}

object CliEntityConverterSyntax extends CliEntityConverterSyntax
