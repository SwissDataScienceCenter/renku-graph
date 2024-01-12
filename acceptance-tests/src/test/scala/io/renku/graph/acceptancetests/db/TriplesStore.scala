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

package io.renku.graph.acceptancetests.db

import cats.effect.IO
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.graph.triplesstore.DatasetTTLs.ProjectsTTL
import io.renku.http.client.BasicAuthCredentials
import io.renku.projectauth.{ProjectAuthData, QueryFilter}
import io.renku.triplesstore._
import io.renku.triplesstore.client.util.{JenaServer, JenaServerSupport}
import org.typelevel.log4cats.Logger

object TriplesStore extends JenaServerSupport {

  override lazy val server: JenaServer = new JenaServer("acceptance_tests", port = 3030)

  lazy val fusekiUrl: FusekiUrl = FusekiUrl(server.conConfig.baseUrl.renderString)

  def startUnsafe(): Unit = server.start()
  def forceStop():   Unit = server.forceStop()

  def start(implicit logger: Logger[IO]): IO[Unit] =
    IO(startUnsafe()) >> logger.info("jena started")

  def findProjectAuth(
      slug: projects.Slug
  )(implicit renkuUrl: RenkuUrl, sqtr: SparqlQueryTimeRecorder[IO], L: Logger[IO]): IO[Option[ProjectAuthData]] =
    projectsTtlDSName
      .flatMap(projectSparqlClient)
      .map(_.asProjectAuthService)
      .use(_.getAll(QueryFilter.all.withSlug(slug)).compile.last)

  private lazy val projectsTtlDSName =
    IO.fromEither(ProjectsTTL.fromTtlFile())
      .map(_.datasetName)
      .toResource

  private def projectSparqlClient(dsName: DatasetName)(implicit sqtr: SparqlQueryTimeRecorder[IO], L: Logger[IO]) =
    ProjectSparqlClient[IO](
      ProjectsConnectionConfig(
        fusekiUrl,
        BasicAuthCredentials.from(
          server.conConfig.basicAuth.getOrElse(throw new Exception("No AuthCredentials for 'project' dataset"))
        ),
        dsName
      )
    )
}
