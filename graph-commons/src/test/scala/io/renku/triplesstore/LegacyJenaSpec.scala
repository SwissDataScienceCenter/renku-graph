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

package io.renku.triplesstore

import cats.effect.std.Random
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.renku.graph.triplesstore.DatasetTTLs.ProjectsTTL
import io.renku.http.client.BasicAuthCredentials
import io.renku.triplesstore.client.http.SparqlClient
import io.renku.triplesstore.client.util.JenaSpec
import org.scalatest.Suite
import org.typelevel.log4cats.Logger

trait LegacyJenaSpec extends JenaSpec {
  self: Suite =>

  def projectsDSConfig(implicit L: Logger[IO]): Resource[IO, ProjectsConnectionConfig] =
    (projectsTtl -> generateName)
      .mapN((origTtl, newName) => updateDSName(origTtl, newName, new ProjectsTTL(_, _)))
      .toResource
      .flatMap(ttl => clientResource.flatMap(datasetResource(ttl)(_).as(projectsConnectionConfig(ttl))))

  def projectsDSResource(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] =
    projectsDSConfig
      .flatMap(conf => SparqlClient[IO](conf.toCC()))

  private lazy val projectsTtl =
    IO.fromEither(ProjectsTTL.fromTtlFile())

  private def generateName =
    Random
      .scalaUtilRandom[IO]
      .flatMap(_.nextIntBounded(1000))
      .map(v => s"${getClass.getSimpleName.toLowerCase}_$v")

  private def updateDSName[C <: DatasetConfigFile](config: C, newName: String, factory: (DatasetName, String) => C): C =
    factory(
      DatasetName(newName),
      config.value.replace(s""""${config.name}"""", s""""$newName"""")
    )

  private def projectsConnectionConfig(ttl: ProjectsTTL) =
    ProjectsConnectionConfig(
      FusekiUrl(server.ccConfig.baseUrl.renderString),
      BasicAuthCredentials.from(
        server.ccConfig.basicAuth.getOrElse(throw new Exception("No AuthCredentials for 'project' dataset"))
      ),
      ttl.datasetName
    )
}
