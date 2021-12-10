/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.activities

import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.{prov, schema}
import io.renku.graph.model.entities.{Activity, Association}
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{entities, users}
import io.renku.rdfstore.SparqlQuery
import io.renku.rdfstore.SparqlQuery.Prefixes

private trait UpdatesCreator {
  def queriesUnlinkingAuthor(activity: entities.Activity, maybeKgAuthor: Option[users.ResourceId]): List[SparqlQuery]
  def queriesUnlinkingAgent(activity:  entities.Activity, maybeKgAuthor: Option[users.ResourceId]): List[SparqlQuery]
}

private object UpdatesCreator extends UpdatesCreator {

  override def queriesUnlinkingAuthor(activity:      Activity,
                                      maybeKgAuthor: Option[users.ResourceId]
  ): List[SparqlQuery] = {
    val activityAuthor = activity.author.resourceId
    Option
      .when(maybeKgAuthor.exists(_ != activityAuthor)) {
        SparqlQuery.of(
          name = "transformation - delete activity author link",
          Prefixes of (schema -> "schema", prov -> "prov"),
          s"""|DELETE {
              |  ${activity.resourceId.showAs[RdfResource]} prov:wasAssociatedWith ?personId
              |}
              |WHERE {
              |  ${activity.resourceId.showAs[RdfResource]} a prov:Activity;
              |                                             prov:wasAssociatedWith ?personId.
              |  ?personId a schema:Person.
              |}
              |""".stripMargin
        )
      }
      .toList
  }

  override def queriesUnlinkingAgent(activity: Activity, maybeKgAgent: Option[users.ResourceId]): List[SparqlQuery] =
    activity.association match {
      case _:     Association.WithRenkuAgent => List.empty
      case assoc: Association.WithPersonAgent =>
        Option
          .when(maybeKgAgent.exists(_ != assoc.agent.resourceId)) {
            SparqlQuery.of(
              name = "transformation - delete association agent link",
              Prefixes of (schema -> "schema", prov -> "prov"),
              s"""|DELETE {
                  |  ${assoc.resourceId.showAs[RdfResource]} prov:agent ?agentId
                  |}
                  |WHERE {
                  |  ${assoc.resourceId.showAs[RdfResource]} a prov:Association;
                  |                                          prov:agent ?agentId.
                  |  ?agentId a schema:Person.
                  |}
                  |""".stripMargin
            )
          }
          .toList
    }
}
