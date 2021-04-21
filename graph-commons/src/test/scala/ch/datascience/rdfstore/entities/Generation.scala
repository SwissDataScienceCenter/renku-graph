/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.entities

import ch.datascience.rdfstore.entities.CommandParameter.{EntityCommandParameter, Output}
import ch.datascience.rdfstore.entities.Generation.Id
import ch.datascience.rdfstore.entities.RunPlan.Id
import ch.datascience.tinytypes.constraints.UUID
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

final case class Generation(id: Id, activity: Activity, commandOutput: EntityCommandParameter with Output)

object Generation {

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {
    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def factory(role: Role, entityFactory: Activity => Entity): Activity => Generation =
    activity => Generation(Id.generate, activity, role, entityFactory(activity))

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Generation] = JsonLDEncoder.instance {
    generation =>
      JsonLD.entity(
        generation.asEntityId,
        EntityTypes of prov / "Generation",
        Reverse.ofJsonLDsUnsafe(prov / "qualifiedGeneration" -> generation.commandOutput.entity.asJsonLD),
        prov / "activity" -> generation.activity.asEntityId.asJsonLD,
        prov / "hadRole"  -> generation.commandOutput.role.asJsonLD
      )
  }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Generation] =
    EntityIdEncoder.instance(generation =>
      generation.activity.asEntityId.asUrlEntityId / "generation" / generation.id / generation.commandOutput.entity.checksum / generation.commandOutput.entity.location
    )
}
