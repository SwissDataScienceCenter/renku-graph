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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.GitLabClientTools
import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class GLDataFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues
    with GitLabClientTools[IO] {

  it should "fetch access token and use it to fetch relevant data from GL" in {

    val path = projectPaths.generateOne

    val accessToken = accessTokens.generateOne
    givenAccessTokenFinding(path, returning = accessToken.some.pure[IO])

    val data = glDataExtracts(path).generateOne
    givenSingleProjectAPICall(path, accessToken, returning = data.some.pure[IO])

    finder.fetchGLData(path).asserting(_ shouldBe data.some)
  }

  it should "return None if access token cannot be found" in {

    val path = projectPaths.generateOne

    givenAccessTokenFinding(path, returning = None.pure[IO])

    finder.fetchGLData(path).asserting(_ shouldBe None)
  }

  it should "return None if GL returns None" in {

    val path = projectPaths.generateOne

    val accessToken = accessTokens.generateOne
    givenAccessTokenFinding(path, returning = accessToken.some.pure[IO])

    givenSingleProjectAPICall(path, accessToken, returning = None.pure[IO])

    finder.fetchGLData(path).asserting(_ shouldBe None)
  }

  it should "decode relevant data from the response with OK status" in {

    val data = glDataExtracts(projectPaths.generateOne).generateOne

    mapResponse(Ok, Request[IO](), Response[IO]().withEntity(data.asJson))
      .asserting(_.value shouldBe data)
  }

  Unauthorized :: Forbidden :: NotFound :: Nil foreach { status =>
    it should show"decode to None for $status status" in {
      mapResponse(status, Request[IO](), Response[IO]())
        .asserting(_ shouldBe None)
    }
  }

  private implicit val glClient:          GitLabClient[IO]      = mock[GitLabClient[IO]]
  private implicit val accessTokenFinder: AccessTokenFinder[IO] = mock[AccessTokenFinder[IO]]
  private lazy val finder = new GLDataFinderImpl[IO]

  private def givenAccessTokenFinding(path: projects.Path, returning: IO[Option[AccessToken]]) =
    (accessTokenFinder
      .findAccessToken(_: projects.Path)(_: projects.Path => String))
      .expects(path, *)
      .returning(returning)

  private def givenSingleProjectAPICall(path:        projects.Path,
                                        accessToken: AccessToken,
                                        returning:   IO[Option[DataExtract]]
  ) = {
    val endpointName: String Refined NonEmpty = "single-project"
    (glClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[DataExtract]])(_: Option[AccessToken]))
      .expects(uri"projects" / path, endpointName, *, accessToken.some)
      .returning(returning)
  }

  private lazy val mapResponse: ResponseMappingF[IO, Option[DataExtract]] = {

    val path = projectPaths.generateOne

    givenAccessTokenFinding(path, returning = accessTokens.generateSome.pure[IO])

    captureMapping(glClient)(finder.fetchGLData(path).unsafeRunSync(),
                             glDataExtracts(having = path).toGeneratorOfOptions
    )
  }

  private implicit lazy val encoder: Encoder[DataExtract.GL] = Encoder.instance { de =>
    json"""{
      "name":                ${de.name},
      "path_with_namespace": ${de.path},
      "visibility":          ${de.visibility},
      "updated_at":          ${de.maybeDateModified},
      "description":         ${de.maybeDesc},
      "topics":              ${de.keywords},
      "avatar_url":          ${de.maybeImage}
    }""".deepDropNullValues
  }
}
