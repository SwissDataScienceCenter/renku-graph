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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.GraphClass
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._

private trait UpsertsCalculator {
  def calculateUpserts(tsData:           DataExtract.TS,
                       glData:           DataExtract,
                       maybePayloadData: Option[DataExtract]
  ): List[SparqlQuery]
}

private object UpsertsCalculator extends UpsertsCalculator {

  override def calculateUpserts(tsData:           DataExtract.TS,
                                glData:           DataExtract,
                                maybePayloadData: Option[DataExtract]
  ): List[SparqlQuery] = Option
    .when(tsData.name != glData.name) {
      List(
        SparqlQuery.ofUnsafe(
          show"$categoryName: update name in Project",
          Prefixes of (renku -> "renku", schema -> "schema"),
          sparql"""|DELETE { GRAPH ?id { ?id schema:name ?name } }
                   |INSERT { GRAPH ?id { ?id schema:name ${glData.name.asObject} } }
                   |WHERE {
                   |  BIND (${tsData.id.asEntityId} AS ?id)
                   |  GRAPH ?id {
                   |    ?id schema:name ?name
                   |  }
                   |}""".stripMargin
        ),
        SparqlQuery.ofUnsafe(
          show"$categoryName: update name in Projects",
          Prefixes of (renku -> "renku", schema -> "schema"),
          sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:name ?name } }
                   |INSERT { GRAPH ${GraphClass.Projects.id} { ?id schema:name ${glData.name.asObject} } }
                   |WHERE {
                   |  BIND (${tsData.id.asEntityId} AS ?id)
                   |  GRAPH ${GraphClass.Projects.id} {
                   |    ?id schema:name ?name
                   |  }
                   |}""".stripMargin
        )
      )
    }
    .sequence
    .flatten
}
