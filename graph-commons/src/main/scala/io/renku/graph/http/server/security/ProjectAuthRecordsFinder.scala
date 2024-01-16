/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.graph.http.server.security

import cats.effect.kernel.Resource
import cats.effect.{Async, Sync}
import cats.syntax.all._
import eu.timepit.refined.auto._
import fs2.io.net.Network
import io.renku.graph.http.server.security.Authorizer.SecurityRecordFinder
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.http.server.security.model
import io.renku.projectauth.{ProjectAuthService, QueryFilter}
import io.renku.triplesstore.{ProjectSparqlClient, ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait ProjectAuthRecordsFinder[F[_]] extends SecurityRecordFinder[F, projects.Slug]

object ProjectAuthRecordsFinder {

  def create[F[_]: Async: Network: Logger: SparqlQueryTimeRecorder](
      renkuUrl: RenkuUrl,
      connCfg:  ProjectsConnectionConfig
  ): Resource[F, ProjectAuthRecordsFinder[F]] =
    ProjectSparqlClient(connCfg).map(c => apply[F](c, renkuUrl))

  def apply[F[_]: Sync: Logger: SparqlQueryTimeRecorder](
      projectSparqlClient: ProjectSparqlClient[F],
      renkuUrl:            RenkuUrl
  ): ProjectAuthRecordsFinder[F] =
    new Impl[F](projectSparqlClient.asProjectAuthService(renkuUrl))

  def apply[F[_]: Sync: SparqlQueryTimeRecorder: Logger](
      projectAuthService: ProjectAuthService[F]
  ): ProjectAuthRecordsFinder[F] =
    new Impl[F](projectAuthService)

  private final class Impl[F[_]: Sync: SparqlQueryTimeRecorder: Logger](
      projectAuthService: ProjectAuthService[F]
  ) extends ProjectAuthRecordsFinder[F] {
    private[this] val timeRecorder = SparqlQueryTimeRecorder[F]
    override def apply(slug: projects.Slug, user: Option[model.AuthUser]): F[List[Authorizer.SecurityRecord]] = {
      val filter = QueryFilter.all.withSlug(slug)
      val data   = projectAuthService.getAll(filter).compile.toList
      timeRecorder.reportTime("project-security-records")(data).map { list =>
        list.map(d =>
          Authorizer.SecurityRecord(
            visibility = d.visibility,
            projectSlug = d.slug,
            allowedPersons = d.members.map(_.gitLabId)
          )
        )
      }
    }
  }
}
