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
import io.renku.graph.model.{plans, projects}
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters.EntityType
import io.renku.knowledgegraph.entities.model.{Entity, MatchingScore}

private case object WorkflowsQuery extends EntityQuery[model.Entity.Workflow] {

  override val entityType: EntityType = EntityType.Workflow

  override val selectVariables =
    Set("?entityType", "?matchingScore", "?wkId", "?name", "?visibilities", "?date", "?maybeDescription", "?keywords")

  override def query(criteria: Endpoint.Criteria) = (criteria.filters whenRequesting entityType) {
    import criteria._
    s"""|{
        |  SELECT ?entityType ?matchingScore ?wkId ?name ?visibilities ?date ?maybeDescription
        |    (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |  WHERE {
        |    {
        |      SELECT ?wkId ?matchingScore ?date (GROUP_CONCAT(DISTINCT ?visibility; separator=',') AS ?visibilities)
        |      WHERE {
        |        {
        |          SELECT ?wkId (MAX(?score) AS ?matchingScore)
        |          WHERE {
        |            (?wkId ?score) text:query (schema:name schema:keywords schema:description '${filters.query}').
        |            ?wkId a prov:Plan
        |          }
        |          GROUP BY ?wkId
        |        }
        |        ?wkId schema:name ?name;
        |              schema:dateCreated ?date;
        |              ^renku:hasPlan ?projectId.
        |        ?projectId renku:projectVisibility ?visibility
        |        ${criteria.maybeOnAccessRights("?projectId", "?visibility")}
        |        ${filters.maybeOnVisibility("?visibility")}
        |        ${filters.maybeOnDateCreated("?date")}
        |      }
        |      GROUP BY ?wkId ?matchingScore ?date
        |    }
        |    BIND ('workflow' AS ?entityType)
        |    ?wkId schema:name ?name.
        |    OPTIONAL { ?wkId schema:description ?maybeDescription }
        |    OPTIONAL { ?wkId schema:keywords ?keyword }
        |  }
        |  GROUP BY ?entityType ?matchingScore ?wkId ?name ?visibilities ?date ?maybeDescription
        |}
        |""".stripMargin
  }

  override def decoder[EE >: Entity.Workflow]: Decoder[EE] = { cursor =>
    import DecodingTools._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val selectBroaderVisibility: List[projects.Visibility] => projects.Visibility = list =>
      list
        .find(_ == projects.Visibility.Public)
        .orElse(list.find(_ == projects.Visibility.Internal))
        .getOrElse(projects.Visibility.Private)

    for {
      matchingScore <- cursor.downField("matchingScore").downField("value").as[MatchingScore]
      name          <- cursor.downField("name").downField("value").as[plans.Name]
      dateCreated   <- cursor.downField("date").downField("value").as[plans.DateCreated]
      visibility <- cursor
                      .downField("visibilities")
                      .downField("value")
                      .as[Option[String]]
                      .flatMap(toListOf[projects.Visibility, projects.Visibility.type](projects.Visibility))
                      .map(selectBroaderVisibility)
      keywords <- cursor
                    .downField("keywords")
                    .downField("value")
                    .as[Option[String]]
                    .flatMap(toListOf[plans.Keyword, plans.Keyword.type](plans.Keyword))
      maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[plans.Description]]
    } yield Entity.Workflow(matchingScore, name, visibility, dateCreated, keywords, maybeDescription)
  }
}
