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

package io.renku.graph.model.testentities

import Entity.OutputEntity
import Generation.Id
import cats.syntax.all._
import io.renku.graph.model.{activities, entities, generations}
import io.renku.tinytypes.constraints.UUID
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

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

  import io.renku.graph.model.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def toEntitiesGeneration(implicit renkuBaseUrl: RenkuBaseUrl): Generation => entities.Generation =
    generation =>
      entities.Generation(
        generations.ResourceId(generation.asEntityId.show),
        activities.ResourceId(generation.activity.asEntityId.show),
        generation.entity.to[entities.Entity.OutputEntity]
      )

  def factory(entityFactory: Generation => OutputEntity): Activity => Generation =
    activity => Generation(Id.generate, activity, entityFactory)

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Generation] =
    JsonLDEncoder.instance(_.to[entities.Generation].asJsonLD)

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Generation] =
    EntityIdEncoder.instance { case generation @ Generation(id, activity, _) =>
      activity.asEntityId.asUrlEntityId / "generation" / id / generation.entity.checksum / generation.entity.location
    }
}
