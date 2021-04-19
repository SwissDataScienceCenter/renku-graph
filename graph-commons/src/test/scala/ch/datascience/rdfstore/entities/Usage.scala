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

import Usage._
import ch.datascience.rdfstore.entities.CommandParameter.{EntityCommandParameter, Input}
import ch.datascience.tinytypes.constraints.UUID
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

final case class Usage(id: Id, activity: Activity, commandInput: EntityCommandParameter with Input)

object Usage {

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { usage =>
      JsonLD.entity(
        usage.asEntityId,
        EntityTypes of (prov / "Usage"),
        prov / "entity"  -> usage.commandInput.entity.asJsonLD,
        prov / "hadRole" -> usage.commandInput.role.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Usage] =
    EntityIdEncoder.instance(usage =>
      usage.activity.asEntityId.asUrlEntityId / "usage" / usage.id / usage.commandInput.entity.checksum / usage.commandInput.entity.location
    )
}
