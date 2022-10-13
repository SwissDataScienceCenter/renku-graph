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
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.knowledgegraph.entities.Endpoint.Criteria
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters.EntityType
import io.renku.knowledgegraph.entities.model.{Entity, MatchingScore}

private case object ProjectsQuery extends EntityQuery[model.Entity.Project] {

  override val entityType: EntityType = EntityType.Project

  override val selectVariables: Set[String] = Set("?entityType",
                                                  "?matchingScore",
                                                  "?name",
                                                  "?path",
                                                  "?visibility",
                                                  "?date",
                                                  "?maybeCreatorName",
                                                  "?maybeDescription",
                                                  "?keywords"
  )

  override def query(criteria: Criteria) = (criteria.filters whenRequesting entityType) {
    import criteria._
    // format: off
    s"""|{
        |  SELECT ?entityType ?matchingScore ?name ?path ?visibility ?date ?maybeCreatorName
        |    ?maybeDescription (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |  WHERE {
        |    ${filters.onQuery(
    s"""|    {
        |      SELECT ?projectId (MAX(?score) AS ?matchingScore)
        |      WHERE {
        |        GRAPH ?g {
        |          {
        |            (?id ?score) text:query (schema:name schema:keywords schema:description renku:projectNamespaces '${filters.query}')
        |          } {
        |            ?id a schema:Project
        |            BIND (?id AS ?projectId)
        |          } UNION {
        |            ?projectId schema:creator ?id;
        |                       a schema:Project.
        |          }
        |        }
        |      }
        |      GROUP BY ?projectId
        |    }
        |""")}
        |    BIND ('project' AS ?entityType)
        |    GRAPH ?projectId {
        |      ?projectId a schema:Project;
        |                 schema:name ?name;
        |                 renku:projectPath ?path;
        |                 renku:projectVisibility ?visibility;
        |                 renku:projectNamespace ?namespace;
        |                 schema:dateCreated ?date.
        |      ${criteria.maybeOnAccessRights("?projectId", "?visibility")}
        |      ${filters.maybeOnVisibility("?visibility")}
        |      ${filters.maybeOnNamespace("?namespace")}
        |      ${filters.maybeOnDateCreated("?date")}
        |      OPTIONAL { 
        |        ?projectId schema:creator ?creatorId.
        |        GRAPH <${GraphClass.Persons.id}> {
        |          ?creatorId schema:name ?maybeCreatorName
        |        }
        |      }
        |      ${filters.maybeOnCreatorName("?maybeCreatorName")}
        |      OPTIONAL { ?projectId schema:description ?maybeDescription }
        |      OPTIONAL { ?projectId schema:keywords ?keyword }
        |    }
        |  }
        |  GROUP BY ?entityType ?matchingScore ?name ?path ?visibility ?date ?maybeCreatorName ?maybeDescription
        |}
        |""".stripMargin
    // format: on
  }

  override def decoder[EE >: Entity.Project]: Decoder[EE] = { implicit cursor =>
    import DecodingTools._
    import cats.syntax.all._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    for {
      matchingScore    <- extract[MatchingScore]("matchingScore")
      path             <- extract[projects.Path]("path")
      name             <- extract[projects.Name]("name")
      visibility       <- extract[projects.Visibility]("visibility")
      dateCreated      <- extract[projects.DateCreated]("date")
      maybeCreatorName <- extract[Option[persons.Name]]("maybeCreatorName")
      keywords <-
        extract[Option[String]]("keywords") >>= toListOf[projects.Keyword, projects.Keyword.type](projects.Keyword)
      maybeDescription <- extract[Option[projects.Description]]("maybeDescription")
    } yield Entity.Project(matchingScore,
                           path,
                           name,
                           visibility,
                           dateCreated,
                           maybeCreatorName,
                           keywords,
                           maybeDescription
    )
  }
}
