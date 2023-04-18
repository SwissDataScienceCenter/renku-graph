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
      sparql"""|SELECT
               |  ?matchingScore
               |  ?entityType
               |  ?datasetName
               |  ?datasetSameAs
               |  ?maybeDateCreated
               |  ?maybeDatePublished
               |  ?maybeDateModified
               |  ?date
               |  ?creatorNames
               |  ?description
               |  ?keywords
               |  ?images
               |  ?projectPath
               |  ?projectVisibility
               |  ?dateViewed
               |
               |  WHERE {
               |    Bind(1.0 AS ?matchingScore)
               |    Bind('dataset' as ?entityType)
               |
               |    Graph renku:PersonViewing {
               |      ?personId a renku:PersonViewing;
               |                renku:viewedDataset ?viewedDataset.
               |
               |      ?viewedDataset renku:dataset ?datasetId;
               |                     renku:dateViewed ?dateViewed
               |    }
               |
               |    Graph schema:Person {
               |      ?personId a schema:Person;
               |                schema:sameAs ?personSameAs.
               |      ?personSameAs schema:additionalType 'GitLab';
               |                    schema:identifier 5624782.
               |    }
               |
               |    {
               |      SELECT
               |        ?datasetSameAs
               |        ?projectId
               |        ?datasetId
               |        (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS ?creatorNames)
               |        (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
               |        (GROUP_CONCAT(DISTINCT ?encodedImageUrl; separator=',') AS ?images)
               |      WHERE {
               |        Graph schema:Dataset {
               |          ?datasetSameAs a renku:DiscoverableDataset;
               |                         renku:datasetProjectLink ?projectLink.
               |
               |          ?projectLink renku:dataset ?datasetId;
               |                       renku:project ?projectId.
               |
               |          Optional {
               |            ?datasetSameAs schema:image ?imageId.
               |            ?imageId schema:position ?imagePosition;
               |                     schema:imageUrl ?imageUrl.
               |            BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
               |          }
               |
               |          Optional {
               |            ?datasetSameAs schema:creator / schema:name ?creatorName.
               |          }
               |          Optional {
               |            ?datasetSameAs schema:keywords ?keyword
               |          }
               |        }
               |      }
               |      GROUP BY ?datasetSameAs ?projectId ?datasetId
               |    }
               |
               |    Graph schema:Dataset {
               |      ?datasetSameAs renku:slug ?datasetName.
               |
               |      Optional {
               |        ?datasetSameAs schema:description ?description.
               |      }
               |
               |      Optional {
               |        ?datasetSameAs schema:dateCreated ?maybeDateCreated.
               |        BIND(xsd:date(substr(str(?maybeDateCreated), 1, 10)) AS ?createdAt)
               |      }
               |      Optional {
               |        ?datasetSameAs schema:datePublished ?maybeDatePublished.
               |        BIND(xsd:date(?maybeDatePublished) AS ?publishedAt)
               |      }
               |      Optional {
               |        ?datasetSameAs schema:dateModified ?maybeDateModified.
               |        BIND(xsd:date(substr(str(?maybeDateModified),1,10)) AS ?modifiedAt)
               |      }
               |      BIND(IF (BOUND(?modifiedAt), ?modifiedAt,
               |               IF (BOUND(?createdAt), ?createdAt, ?publishedAt)) AS ?date
               |           )
               |    }
               |
               |    Graph ?projectId {
               |      ?projectId a schema:Project;
               |                 renku:projectPath ?projectPath;
               |                 renku:projectVisibility ?projectVisibility.
               |    }
               |  }
        """.stripMargin
    )

}
