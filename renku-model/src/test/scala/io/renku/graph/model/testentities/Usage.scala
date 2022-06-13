/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.testentities

import Usage.Id
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.noDashUuid
import io.renku.graph.model.{RenkuUrl, entities, usages}
import io.renku.tinytypes.constraints.UUID
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

final case class Usage(id: Id, activity: Activity, entity: Entity)

object Usage {

  def factory(entity: Entity): Activity => Usage =
    Usage(Id.generate, _, entity)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID[Id] {
    def generate: Id = noDashUuid.generateAs(Id)
  }

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def toEntitiesUsage(implicit renkuUrl: RenkuUrl): Usage => entities.Usage = usage =>
    entities.Usage(usages.ResourceId(usage.asEntityId.show), usage.entity.to[entities.Entity])

  implicit def encoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[Usage] =
    JsonLDEncoder.instance(_.to[entities.Usage].asJsonLD)

  implicit def entityIdEncoder(implicit renkuUrl: RenkuUrl): EntityIdEncoder[Usage] =
    EntityIdEncoder.instance { case Usage(id, activity, entity) =>
      activity.asEntityId.asUrlEntityId / "usage" / id / entity.checksum / entity.location
    }
}
