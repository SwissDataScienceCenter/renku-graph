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

import ch.datascience.graph.config.GitLabApiUrl
import ch.datascience.rdfstore.entities.Entity.InputEntity
import ch.datascience.rdfstore.entities.Usage.Id
import ch.datascience.tinytypes.constraints.UUID
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

final case class Usage(id: Id, activity: Activity, entity: InputEntity)

object Usage {

  def factory(entity: InputEntity): Activity => Usage =
    Usage(Id.generate, _, entity)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {
    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { case usage @ Usage(_, _, entity) =>
      JsonLD.entity(
        usage.asEntityId,
        EntityTypes of (prov / "Usage"),
        prov / "entity" -> entity.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Usage] =
    EntityIdEncoder.instance { case Usage(id, activity, entity) =>
      activity.asEntityId.asUrlEntityId / "usage" / id / entity.checksum / entity.location
    }

}
