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

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import fs2.io.net.Network
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.projects.{Slug, Visibility}
import io.renku.projectauth.{ProjectAuthData, ProjectAuthService, ProjectMember}
import io.renku.triplesstore.ProjectsConnectionConfig
import io.renku.triplesstore.client.http.{RowDecoder, SparqlClient}
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait ProjectAuthSync[F[_]] {
  def syncProject(slug: Slug, members: Set[ProjectMember]): F[Unit]
  def syncProject(data: ProjectAuthData): F[Unit]
}

object ProjectAuthSync {

  def resource[F[_]: Async: Logger: Network](cc: ProjectsConnectionConfig)(implicit renkuUrl: RenkuUrl) =
    ProjectSparqlClient[F](cc).map(apply[F])

  def apply[F[_]: Sync](
      sparqlClient: ProjectSparqlClient[F]
  )(implicit renkuUrl: RenkuUrl): ProjectAuthSync[F] =
    new Impl[F](ProjectAuthService[F](sparqlClient, renkuUrl), sparqlClient)

  private final class Impl[F[_]: Sync](
      projectAuthService: ProjectAuthService[F],
      sparqlClient:       ProjectSparqlClient[F]
  ) extends ProjectAuthSync[F] {
    implicit val logger: Logger[F] = Slf4jLogger.getLogger[F]
    private[this] val visibilityFinder: VisibilityFinder[F] =
      new VisibilityFinder[F](sparqlClient)

    override def syncProject(slug: Slug, members: Set[ProjectMember]): F[Unit] =
      logger.warn(s"Start looking for visibility for $slug") *>
        visibilityFinder.find(slug).flatMap {
          case Some(vis) => syncProject(ProjectAuthData(slug, members, vis))
          case None      => ().pure[F]
        }

    override def syncProject(data: ProjectAuthData): F[Unit] =
      logger.warn(s"Syncing project auth using $data") *>
        projectAuthService.update(data)
  }

  // Hm, should we get this from gitlab? TODO
  private final class VisibilityFinder[F[_]: MonadThrow](sparqlClient: SparqlClient[F]) {
    def find(slug: Slug): F[Option[Visibility]] =
      sparqlClient
        .queryDecode[Visibility](sparql"""PREFIX schema: <http://schema.org/>
                                         |PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>
                                         |
                                         |SELECT ?visibility
                                         |WHERE {
                                         |  BIND (${slug.asObject} AS ?slug)
                                         |  Graph ?id {
                                         |    ?id a schema:Project;
                                         |        renku:projectPath ?slug;
                                         |        renku:projectVisibility ?visibility.
                                         |  }
                                         |}
                                         |""".stripMargin)
        .map(_.headOption)

    implicit def decoder: RowDecoder[Visibility] =
      RowDecoder.forProduct1[Visibility, Visibility]("visibility")(identity)(Visibility.jsonDecoder)
  }
}
