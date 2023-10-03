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
import io.renku.graph.model.entities.Person
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{GraphClass, Schemas}
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.sparql.VarName
import io.renku.triplesstore.client.syntax.FragmentStringContext

object DatasetQuery extends (Criteria => Option[SparqlQuery]) {
  private[this] val v            = Variables.Dataset
  private[this] val authSnippets = SparqlSnippets(VarName("projectId"))

  def apply(criteria: Criteria): Option[SparqlQuery] =
    Option.when(criteria.forType(EntityType.Dataset))(makeQuery(criteria))

  def makeQuery(criteria: Criteria): SparqlQuery =
    SparqlQuery.of(
      name = "recent-entity projects",
      Prefixes.of(Schemas.prov -> "prov", Schemas.renku -> "renku", Schemas.schema -> "schema", Schemas.xsd -> "xsd"),
      sparql"""|SELECT DISTINCT
               |  ${v.all}
               |
               |  WHERE {
               |    Bind(1.0 AS ${v.matchingScore})
               |    Bind('dataset' as ${v.entityType})
               |
               |    Graph ${GraphClass.PersonViewings.id} {
               |      ?personId a renku:PersonViewing;
               |                renku:viewedDataset ?viewedDataset.
               |
               |      ?viewedDataset renku:dataset ?datasetId;
               |                     renku:dateViewed ${v.viewedDate}
               |    }
               |    Graph ${GraphClass.Datasets.id} {
               |      ${v.datasetSameAs} a renku:DiscoverableDataset;
               |                         renku:datasetProjectLink ?projectLink.
               |
               |      ?projectLink renku:dataset ?datasetId;
               |                    renku:project ?projectId.
               |    }
               |
               |    ${authSnippets.visibleProjects(Some(criteria.authUser.id), Visibility.all)}
               |    
               |    Graph ${GraphClass.Persons.id} {
               |      ?personId a schema:Person;
               |                schema:sameAs ?personSameAs.
               |      ?personSameAs schema:additionalType ${Person.gitLabSameAsAdditionalType};
               |                    schema:identifier ${criteria.authUser.id.value}.
               |    }
               |
               |    {
               |      SELECT DISTINCT
               |        ${v.datasetSameAs}
               |        (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS ${v.creatorNames})
               |        (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ${v.keywords})
               |        (GROUP_CONCAT(DISTINCT ?encodedImageUrl; separator=',') AS ${v.images})
               |      WHERE {
               |        Graph ${GraphClass.Datasets.id} {
               |          ${v.datasetSameAs} a renku:DiscoverableDataset;
               |
               |          Optional {
               |            ${v.datasetSameAs} schema:image ?imageId.
               |            ?imageId schema:position ?imagePosition;
               |                     schema:contentUrl ?imageUrl.
               |            BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
               |          }
               |
               |          Optional {
               |            ${v.datasetSameAs} schema:creator ?creatorId.
               |            GRAPH ${GraphClass.Persons.id} {
               |              ?creatorId schema:name ?creatorName
               |            }
               |          }
               |          Optional {
               |            ${v.datasetSameAs} schema:keywords ?keyword
               |          }
               |        }
               |      }
               |      GROUP BY ${v.datasetSameAs}
               |    }
               |
               |    Graph ${GraphClass.Datasets.id} {
               |      ${v.datasetSameAs} renku:slug ${v.datasetName}.
               |
               |      Optional {
               |        ${v.datasetSameAs} schema:description ${v.description}.
               |      }
               |
               |      Optional {
               |        ${v.datasetSameAs} schema:dateCreated ${v.dateCreated}.
               |        BIND(xsd:date(substr(str(${v.dateCreated}), 1, 10)) AS ?createdAt)
               |      }
               |      Optional {
               |        ${v.datasetSameAs} schema:datePublished ${v.datePublished}.
               |        BIND(xsd:date(${v.datePublished}) AS ?publishedAt)
               |      }
               |      Optional {
               |        ${v.datasetSameAs} schema:dateModified ${v.dateModified}.
               |        BIND(xsd:date(substr(str(${v.dateModified}),1,10)) AS ?modifiedAt)
               |      }
               |      BIND(IF (BOUND(?modifiedAt), ?modifiedAt,
               |               IF (BOUND(?createdAt), ?createdAt, ?publishedAt)) AS ${v.date}
               |           )
               |    }
               |
               |    Graph ?projectId {
               |      ?projectId a schema:Project;
               |                 renku:projectPath ${v.projectSlug};
               |                 renku:projectVisibility ${v.projectVisibility}.
               |    }
               |  }
        """.stripMargin
    )

}
