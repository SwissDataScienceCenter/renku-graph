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

package io.renku.entities.viewings.collector.projects.viewed

import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.triplesstore.TSClient

private trait EventDeduplicator[F[_]] {
  def deduplicate(projectId: projects.ResourceId): F[Unit]
}

private object EventDeduplicator {
  def apply[F[_]](tsClient: TSClient[F]): EventDeduplicator[F] = new EventDeduplicatorImpl[F](tsClient)
}

private class EventDeduplicatorImpl[F[_]](tsClient: TSClient[F]) extends EventDeduplicator[F] {

  import eu.timepit.refined.auto._
  import io.renku.graph.model.GraphClass
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._
  import tsClient.updateWithNoResult

  override def deduplicate(projectId: projects.ResourceId): F[Unit] = updateWithNoResult(
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: deduplicate",
      Prefixes of renku -> "renku",
      s"""|DELETE {
          |  GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} { ?id renku:dateViewed ?date }
          |}
          |WHERE {
          |  GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} {
          |    BIND (${projectId.asEntityId.sparql} AS ?id)
          |    {
          |      SELECT ?id (MAX(?date) AS ?maxDate)
          |      WHERE {
          |        ?id renku:dateViewed ?date
          |      }
          |      GROUP BY ?id
          |      HAVING (COUNT(?date) > 1)
          |    }
          |    ?id renku:dateViewed ?date.
          |    FILTER (?date != ?maxDate)
          |  }
          |}
          |""".stripMargin
    )
  )
}
