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

package io.renku.triplesgenerator.events.consumers

import cats.effect._
import fs2.io.net.Network
import io.renku.jsonld.JsonLD
import io.renku.triplesstore.ProjectsConnectionConfig
import io.renku.triplesstore.client.http.{Retry, SparqlClient, SparqlQuery, SparqlUpdate}
import org.typelevel.log4cats.Logger

/** SparQL client fixed to the `projects` dataset. */
trait ProjectSparqlClient[F[_]] extends SparqlClient[F]

object ProjectSparqlClient {
  def apply[F[_]: Network: Async: Logger](
      cc:       ProjectsConnectionConfig,
      retryCfg: Retry.RetryConfig = Retry.RetryConfig.default
  ): Resource[F, ProjectSparqlClient[F]] = {
    val cfg = cc.toCC(Some(retryCfg))
    SparqlClient[F](cfg).map(c =>
      new ProjectSparqlClient[F] {
        override def update(request: SparqlUpdate) = c.update(request)
        override def upload(data:    JsonLD)       = c.upload(data)
        override def query(request:  SparqlQuery)  = c.query(request)
      }
    )
  }
}
