/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.graph.model.cli

import io.renku.cli.model._
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
