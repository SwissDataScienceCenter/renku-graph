/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.entities.search

import io.circe.Decoder
import io.renku.entities.search.Criteria.Filters.EntityType
import io.renku.triplesstore.ResultsDecoder
import io.renku.triplesstore.client.sparql.{Fragment, VarName}

private[entities] trait EntityQuery[+E <: model.Entity] extends ResultsDecoder with EntityQueryVars {
  val entityType:      EntityType
  val selectVariables: Set[String]
  def query(criteria: Criteria): Option[Fragment]
  def decoder[EE >: E]: Decoder[EE]

  def getDecoder[EE >: E](entityType: EntityType): Option[Decoder[EE]] =
    Option.when(entityType == this.entityType)(decoder[EE])
}

private[entities] object EntityQueryVars extends EntityQueryVars
private[entities] trait EntityQueryVars {
  val entityTypeVar = VarName("entityType")
}
