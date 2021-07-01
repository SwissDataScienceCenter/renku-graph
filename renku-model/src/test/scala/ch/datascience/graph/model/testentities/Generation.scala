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

package ch.datascience.graph.model.testentities

import ch.datascience.graph.model.GitLabApiUrl
import Entity.OutputEntity
import Generation.Id
import ch.datascience.tinytypes.constraints.UUID
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

final case class Generation(id: Id, activity: Activity, entityFactory: Generation => OutputEntity) {
  lazy val entity: OutputEntity = entityFactory(this)
}

object Generation {

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {
    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }

  import ch.datascience.graph.model.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def factory(entityFactory: Generation => OutputEntity): Activity => Generation =
    activity => Generation(Id.generate, activity, entityFactory)

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Generation] =
    JsonLDEncoder.instance { generation =>
      JsonLD.entity(
        generation.asEntityId,
        EntityTypes of prov / "Generation",
        Reverse.ofJsonLDsUnsafe(prov / "qualifiedGeneration" -> generation.entity.asJsonLD),
        prov / "activity" -> generation.activity.asEntityId.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Generation] =
    EntityIdEncoder.instance { case generation @ Generation(id, activity, _) =>
      activity.asEntityId.asUrlEntityId / "generation" / id / generation.entity.checksum / generation.entity.location
    }
}
