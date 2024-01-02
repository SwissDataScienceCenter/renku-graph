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
import io.renku.entities.search.model.{Entity, MatchingScore}
import io.renku.graph.model.entities.Person.gitLabSameAsAdditionalType
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.client.sparql.{Fragment, VarName}
import io.renku.triplesstore.client.syntax._

private case object PersonsQuery extends EntityQuery[model.Entity.Person] {

  override val entityType: EntityType = EntityType.Person

  override val selectVariables: Set[String] = Set("?entityType", "?matchingScore", "?name")

  override def query(criteria: Criteria): Option[Fragment] =
    (criteria.filters whenRequesting (entityType, criteria.filters.withNoOrPublicVisibility, criteria.filters.namespaces.isEmpty, criteria.filters.maybeSince.isEmpty, criteria.filters.maybeUntil.isEmpty)) {
      import criteria._
      sparql"""|{
               |  SELECT DISTINCT ?entityType ?matchingScore ?name
               |  WHERE {
               |    {
               |      SELECT (SAMPLE(?id) AS ?personId) ?name (MAX(?score) AS ?matchingScore)
               |      WHERE {
               |        ${textPart(criteria.filters)}
               |
               |        GRAPH ${GraphClass.Persons.id} {
               |          ?id a schema:Person;
               |              schema:name ?name.
               |          ${filterOnRoles(criteria)}
               |        }
               |        ${filters.maybeOnCreatorName(VarName("name"))}
               |      }
               |      GROUP BY ?name
               |    }
               |    BIND ('person' AS ?entityType)
               |  }
               |}
               |""".stripMargin
    }

  private def textPart(filters: Criteria.Filters) =
    filters.onQuery(
      snippet = fr"""(?id ?score) text:query (schema:name ${filters.query}).""",
      matchingScoreVariableName = VarName("score")
    )

  private def filterOnRoles(criteria: Criteria): Fragment = criteria.maybeUser -> criteria.filters.roles match {
    case Some(AuthUser(id, _)) -> roles if roles contains projects.Role.Owner =>
      fr"""|?id schema:sameAs ?sameAsId.
           |?sameAsId schema:additionalType ${gitLabSameAsAdditionalType.asTripleObject};
           |          schema:identifier ${id.asObject}.
           |""".stripMargin
    case _ -> roles if roles.nonEmpty =>
      // this is a hack to filter out all the persons
      // the assumption is that the caller cannot have an explicit Maintainer or Reader role on a Person
      fr"""|FILTER EXISTS {
           |  ?id renku:nonexisting true.
           |}
           |""".stripMargin
    case _ => Fragment.empty
  }

  override def decoder[EE >: Entity.Person]: Decoder[EE] = { implicit cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    for {
      matchingScore <- extract[MatchingScore]("matchingScore")
      name          <- extract[persons.Name]("name")
    } yield Entity.Person(matchingScore, name)
  }
}
