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

package ch.datascience.graph.model

import cats.kernel.Semigroup
import ch.datascience.graph.model.datasets.SameAs
import ch.datascience.graph.model.testentities.generators.EntitiesGenerators
import ch.datascience.tinytypes.constraints._
import io.renku.jsonld._

package object testentities extends Schemas with EntitiesGenerators with ModelOps {

  type ::~[+A, +B] = (A, B)
  object ::~ {
    def unapply[A, B](t: A ::~ B): Some[A ::~ B] = Some(t)
  }

  implicit val renkuBaseUrlToEntityId: RenkuBaseUrl => EntityId = url => EntityId of url.value

  private implicit lazy val sameAsToPathSegment: SameAs => List[PathSegment] = sameAs => List(PathSegment(sameAs.value))

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val asUrlEntityId: UrlfiedEntityId = UrlfiedEntityId(entityId.value.toString)
  }

  implicit val reverseSemigroup: Semigroup[Reverse] =
    (x: Reverse, y: Reverse) => Reverse(x.properties ++ y.properties)
}
