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

package io.renku.projectauth

import cats.effect._
import fs2.Pipe
import fs2.io.net.Network
import io.renku.graph.model.{RenkuUrl, Schemas}
import io.renku.jsonld.NamedGraph
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.http.{ConnectionConfig, SparqlClient}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

/** Manage authorization data for projects and members. */
trait ProjectAuthService[F[_]] {

  def update(data: ProjectAuthData): F[Unit]

  def updateAll: Pipe[F, ProjectAuthData, Nothing]

}

object ProjectAuthService {

  def apply[F[_]: Async: Network: Logger](
      connectionConfig: ConnectionConfig,
      timeout:          Duration = 20.minutes
  )(implicit renkuUrl: RenkuUrl): Resource[F, ProjectAuthService[F]] =
    SparqlClient(connectionConfig, timeout).map(c => apply[F](c, renkuUrl))

  def apply[F[_]](client: SparqlClient[F], renkuUrl: RenkuUrl): ProjectAuthService[F] =
    new Impl[F](client)(renkuUrl)

  private final class Impl[F[_]](sparqlClient: SparqlClient[F])(implicit renkuUrl: RenkuUrl)
      extends ProjectAuthService[F] {
    private[this] val graph = Schemas.renku / "ProjectAuth"

    override def update(data: ProjectAuthData): F[Unit] = {
      val jsonld = NamedGraph.fromJsonLDsUnsafe(graph, data.asJsonLD)
      sparqlClient.upload(jsonld)
    }

    override def updateAll: Pipe[F, ProjectAuthData, Nothing] =
      _.chunks
        .map(chunk =>
          chunk.toNel match { // TODO improve that weird ergonomics for NamedGraph in jsonld4s
            case Some(nel) => NamedGraph.fromJsonLDsUnsafe(graph, nel.head.asJsonLD, nel.tail.map(_.asJsonLD): _*)
            case None      => NamedGraph(graph, Seq.empty)
          }
        )
        .evalMap(sparqlClient.upload)
        .drain
  }
}
