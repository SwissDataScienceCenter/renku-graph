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

package io.renku.entities.searchgraphs.projects.commands

import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.{GraphClass, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._

private[projects] object ProjectInfoDeleteQuery {

  def apply(projectId: projects.ResourceId): SparqlQuery =
    SparqlQuery.ofUnsafe(
      "delete project info",
      Prefixes of schema -> "schema",
      sparql"""|DELETE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?projId ?projPred ?projObj.
               |  }
               |}
               |WHERE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    BIND (${projectId.asEntityId} AS ?projId)
               |    ?projId ?projPred ?projObj.
               |  }
               |}
               |""".stripMargin
    )
}
