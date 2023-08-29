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
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.http.server.security.Authorizer.SecurityRecordFinder
import io.renku.graph.model.datasets
import io.renku.http.server.security.model
import io.renku.triplesstore.ProjectSparqlClient
import io.renku.triplesstore.client.http.RowDecoder
import io.renku.triplesstore.client.syntax._

trait DatasetIdRecordsFinder2[F[_]] extends SecurityRecordFinder[F, datasets.Identifier]

object DatasetIdRecordsFinder2 {

  private class Impl[F[_]: Sync](projectSparqlClient: ProjectSparqlClient[F]) extends DatasetIdRecordsFinder2[F] {
    override def apply(id: datasets.Identifier, user: Option[model.AuthUser]): F[List[Authorizer.SecurityRecord]] =
      projectSparqlClient.queryDecode[Authorizer.SecurityRecord](query(id))

    private def query(id: datasets.Identifier) =
      sparql"""$id
              |""".stripMargin

    implicit def rowDecoder: RowDecoder[Authorizer.SecurityRecord] = ???
  }
}
