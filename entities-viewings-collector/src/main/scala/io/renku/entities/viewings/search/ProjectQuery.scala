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
import io.renku.triplesstore.client.syntax._

object ProjectQuery extends (Criteria => Option[SparqlQuery]) {
  private[this] val v            = Variables.Project
  private[this] val authSnippets = SparqlSnippets(VarName("projectId"), v.visibility)

  def apply(criteria: Criteria): Option[SparqlQuery] =
    Option.when(criteria.forType(EntityType.Project))(makeQuery(criteria))

  def makeQuery(criteria: Criteria): SparqlQuery =
    SparqlQuery.of(
      name = "recent-entity projects",
      Prefixes.of(Schemas.prov -> "prov", Schemas.renku -> "renku", Schemas.schema -> "schema", Schemas.xsd -> "xsd"),
      sparql"""|SELECT DISTINCT
               |  ${v.all.dropRight(2)}
               |  (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ${v.keywords})
               |  (GROUP_CONCAT(?encodedImageUrl; separator=',') AS ${v.images})
               |  WHERE {
               |    BIND (1.0 AS ${v.matchingScore})
               |    BIND ('project' AS ${v.entityType})
               |
               |    graph ${GraphClass.PersonViewings.id} {
               |      ?personId a renku:PersonViewing;
               |                renku:viewedProject ?viewedProject.
               |
               |      ?viewedProject a renku:ViewedProject;
               |                     renku:project ?projectId;
               |                     renku:dateViewed ${v.viewedDate}
               |    }
               |    Graph ${GraphClass.Persons.id} {
               |      ?personId a schema:Person;
               |                schema:sameAs ?personSameAs.
               |      ?personSameAs schema:additionalType ${Person.gitLabSameAsAdditionalType};
               |                    schema:identifier ${criteria.authUser.id.value}.
               |    }
               |
               |    ${authSnippets.visibleProjects(Some(criteria.authUser.id), Visibility.all)}
               |
               |    Graph ?projectId {
               |      ?projectId a schema:Project;
               |               schema:name ${v.projectName};
               |               renku:projectPath ${v.projectSlug};
               |               renku:projectVisibility ${v.visibility};
               |               schema:dateModified ${v.dateModified};
               |               schema:dateCreated ${v.dateCreated}.
               |
               |      Optional {
               |        ?projectId schema:creator ?creator.
               |        Graph ${GraphClass.Persons.id} {
               |          ?creator schema:name ${v.creatorNames}
               |        }
               |      }
               |      Optional {
               |        ?projectId schema:description ${v.description}
               |      }
               |      Optional {
               |        ?projectId schema:keywords ?keyword
               |      }
               |      Optional {
               |        ?projectId schema:image ?imageId.
               |        ?imageId schema:position ?imagePosition;
               |                 schema:contentUrl ?imageUrl.
               |        BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
               |      }
               |    }
               |  }
               |  GROUP BY ${v.all.dropRight(2)}
               |""".stripMargin
    )
}
