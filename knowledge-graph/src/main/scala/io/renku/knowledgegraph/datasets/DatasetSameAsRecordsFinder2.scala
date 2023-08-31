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

package io.renku.knowledgegraph.datasets

import cats.effect.kernel.Sync
import fs2.Stream
import eu.timepit.refined.auto._
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.model.datasets
import io.renku.http.server.security.model
import io.renku.jsonld.EntityId
import io.renku.projectauth.util.ProjectAuthDataRow
import io.renku.triplesstore.{ProjectSparqlClient, SparqlQueryTimeRecorder}
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

trait DatasetSameAsRecordsFinder2[F[_]] extends Authorizer.SecurityRecordFinder[F, datasets.SameAs]

object DatasetSameAsRecordsFinder2 {

  def apply[F[_]: Sync: Logger: SparqlQueryTimeRecorder](
      projectSparqlClient: ProjectSparqlClient[F]
  ): DatasetSameAsRecordsFinder2[F] =
    new Impl[F](projectSparqlClient)

  private class Impl[F[_]: Sync: Logger: SparqlQueryTimeRecorder](projectSparqlClient: ProjectSparqlClient[F])
      extends DatasetSameAsRecordsFinder2[F] {
    private[this] val timeRecorder = SparqlQueryTimeRecorder[F]

    override def apply(sameAs: datasets.SameAs, user: Option[model.AuthUser]): F[List[Authorizer.SecurityRecord]] =
      Stream
        .evals(runQuery(sameAs))
        .through(ProjectAuthDataRow.collect)
        .map(p => Authorizer.SecurityRecord(p.visibility, p.slug, p.members.map(_.gitLabId)))
        .compile
        .toList

    private def runQuery(sameAs: datasets.SameAs) =
      timeRecorder.reportTime("security-dataset-sameAs")(
        projectSparqlClient.queryDecode[ProjectAuthDataRow](query(sameAs))
      )

    private def query(sameAs: datasets.SameAs) =
      sparql"""PREFIX schema: <http://schema.org/>
              |PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>
              |
              |  select distinct ?slug ?visibility ?memberRole
              |  where {
              |    bind (${EntityId.of(sameAs.value)} as ?dsSameAs).
              |    graph schema:Dataset {
              |      ?dsSameAs a renku:DiscoverableDataset;
              |                renku:datasetProjectLink / renku:project ?projectId.
              |    }
              |    graph renku:ProjectAuth {
              |      ?projectId a schema:Project;
              |                 renku:slug ?slug;
              |                 renku:visibility ?visibility.
              |      Optional {
              |        ?projectId renku:memberRole ?memberRole.
              |      }
              |    }
              |  }
              |  ORDER BY ?slug
              |""".stripMargin
  }
}
