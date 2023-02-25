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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.activities

import eu.timepit.refined.auto._
import io.renku.graph.model._
import io.renku.graph.model.Schemas.{prov, schema}
import io.renku.graph.model.entities.{Activity, ActivityLens}
import io.renku.graph.model.views.RdfResource
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes

private trait UpdatesCreator {
  def queriesUnlinkingAuthors(projectId: projects.ResourceId,
                              activity:  entities.Activity,
                              kgAuthors: Set[persons.ResourceId]
  ): List[SparqlQuery]
  def queriesUnlinkingAgents(projectId: projects.ResourceId,
                             activity:  entities.Activity,
                             kgAgents:  Set[persons.ResourceId]
  ): List[SparqlQuery]
}

private object UpdatesCreator extends UpdatesCreator {

  override def queriesUnlinkingAuthors(projectId: projects.ResourceId,
                                       activity:  Activity,
                                       kgAuthors: Set[persons.ResourceId]
  ): List[SparqlQuery] = {
    val activityAuthor = activity.author.resourceId
    Option
      .when((kgAuthors.size > 1) || !(kgAuthors forall (_ == activityAuthor))) {
        SparqlQuery.of(
          name = "transformation - delete activity author link",
          Prefixes of (schema -> "schema", prov -> "prov"),
          s"""|DELETE { GRAPH <${GraphClass.Project.id(projectId)}> { ?activityId prov:wasAssociatedWith ?personId } }
              |WHERE {
              |  BIND (${activity.resourceId.showAs[RdfResource]} AS ?activityId)
              |  GRAPH <${GraphClass.Project.id(projectId)}> {
              |    ?activityId a prov:Activity;
              |                prov:wasAssociatedWith ?personId
              |  }
              |  GRAPH <${GraphClass.Persons.id}> {
              |    ?personId a schema:Person
              |  }
              |}
              |""".stripMargin
        )
      }
      .toList
  }

  override def queriesUnlinkingAgents(projectId: projects.ResourceId,
                                      activity:  Activity,
                                      kgAgents:  Set[persons.ResourceId]
  ): List[SparqlQuery] =
    ActivityLens.activityAssociationAgent
      .get(activity)
      .map(agent =>
        Option
          .when((kgAgents.size > 1) || !(kgAgents forall (_ == agent.resourceId))) {
            SparqlQuery.of(
              name = "transformation - delete association agent link",
              Prefixes of (schema -> "schema", prov -> "prov"),
              s"""|DELETE { GRAPH <${GraphClass.Project.id(projectId)}> { ?assocId prov:agent ?agentId } }
                  |WHERE {
                  |  BIND (${activity.association.resourceId.showAs[RdfResource]} AS ?assocId)
                  |  GRAPH <${GraphClass.Project.id(projectId)}> {
                  |    ?assocId a prov:Association;
                  |             prov:agent ?agentId
                  |  }
                  |  GRAPH <${GraphClass.Persons.id}> {
                  |    ?agentId a schema:Person
                  |  }
                  |}
                  |""".stripMargin
            )
          }
          .toList
      )
      .getOrElse(List.empty)

}
