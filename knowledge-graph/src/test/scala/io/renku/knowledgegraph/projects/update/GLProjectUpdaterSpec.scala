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

package io.renku.knowledgegraph.projects.update

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.GitLabClientTools
import org.http4s.Method.PUT
import org.http4s.Status.Ok
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri, UrlForm}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class GLProjectUpdaterSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with GitLabClientTools[IO] {

  it should "call GL's PUT gl/projects/:slug" in {

    val path        = projectPaths.generateOne
    val newValues   = newValuesGen.generateOne
    val accessToken = accessTokens.generateOne

    givenEditProjectAPICall(path, newValues, accessToken, returning = ().pure[IO])

    finder.updateProject(path, newValues, accessToken).assertNoException
  }

  it should "succeed if PUT gl/projects/:slug returns 200 OK" in {
    mapResponse(Ok, Request[IO](), Response[IO]()).assertNoException
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new GLProjectUpdaterImpl[IO]

  private def givenEditProjectAPICall(path:        projects.Path,
                                      newValues:   NewValues,
                                      accessToken: AccessToken,
                                      returning:   IO[Unit]
  ) = {
    val endpointName: String Refined NonEmpty = "edit-project"
    (glClient
      .put(_: Uri, _: String Refined NonEmpty, _: UrlForm)(_: ResponseMappingF[IO, Unit])(_: Option[AccessToken]))
      .expects(uri"projects" / path,
               endpointName,
               UrlForm("visibility" -> newValues.visibility.value),
               *,
               accessToken.some
      )
      .returning(returning)
  }

  private lazy val mapResponse: ResponseMappingF[IO, Unit] =
    captureMapping(glClient)(
      finder
        .updateProject(projectPaths.generateOne, newValuesGen.generateOne, accessTokens.generateOne)
        .unsafeRunSync(),
      (),
      method = PUT
    )
}
