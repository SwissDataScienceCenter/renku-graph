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

package io.renku.knowledgegraph.entities
package finder

import io.circe.Decoder
import io.renku.graph.model.persons
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters.EntityType
import io.renku.knowledgegraph.entities.model.{Entity, MatchingScore}

private case object PersonsQuery extends EntityQuery[model.Entity.Person] {

  override val entityType: EntityType = EntityType.Person

  override val selectVariables = Set("?entityType", "?matchingScore", "?name")

  override def query(criteria: Endpoint.Criteria) =
    (criteria.filters whenRequesting (entityType, criteria.filters.withNoOrPublicVisibility, criteria.filters.maybeSince.isEmpty)) {
      import criteria._
      s"""|{
          |  SELECT DISTINCT ?entityType ?matchingScore ?name
          |  WHERE {
          |    {
          |      SELECT (SAMPLE(?id) AS ?personId) ?name (MAX(?score) AS ?matchingScore)
          |      WHERE {
          |        (?id ?score) text:query (schema:name '${filters.query}').
          |        ?id a schema:Person;
          |            schema:name ?name.
          |        ${filters.maybeOnCreatorName("?name")}
          |      }
          |      GROUP BY ?name
          |    }
          |    BIND ('person' AS ?entityType)
          |  }
          |}
          |""".stripMargin
    }

  override def decoder[EE >: Entity.Person]: Decoder[EE] = { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    for {
      matchingScore <- cursor.downField("matchingScore").downField("value").as[MatchingScore]
      name          <- cursor.downField("name").downField("value").as[persons.Name]
    } yield Entity.Person(matchingScore, name)
  }
}
