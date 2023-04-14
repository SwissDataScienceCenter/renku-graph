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

package io.renku.entities.viewings.search

import eu.timepit.refined.auto._
import io.renku.entities.viewings.search.RecentEntitiesFinder.{Criteria, EntityType}
import io.renku.graph.model.Schemas
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax.FragmentStringContext

object DatasetQuery extends (Criteria => Option[SparqlQuery]) {
  private[this] val v = Variables

  def apply(criteria: Criteria): Option[SparqlQuery] =
    Option.when(criteria.forType(EntityType.Dataset))(makeQuery(criteria))

  def makeQuery(criteria: Criteria): SparqlQuery =
    SparqlQuery.of(
      name = "recent-entity projects",
      Prefixes.of(Schemas.prov -> "prov", Schemas.renku -> "renku", Schemas.schema -> "schema", Schemas.xsd -> "xsd"),
      sparql"""|SELECT $criteria
        """.stripMargin
    )

}
