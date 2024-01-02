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

package io.renku.knowledgegraph.projects.update

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators.userAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectGitHttpUrls, projectSlugs}
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.GitLabClientTools
import org.http4s.Status.{NotFound, Ok}
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectGitUrlFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues
    with GitLabClientTools[IO] {

  it should "call GL's GET gl/projects/:slug and return the http git url" in {

    val slug        = projectSlugs.generateOne
    val accessToken = userAccessTokens.generateOne
    val maybeGitUrl = projectGitHttpUrls.generateOption

    givenFetchProjectGitUrl(
      slug,
      accessToken,
      returning = maybeGitUrl.pure[IO]
    )

    finder.findGitUrl(slug, accessToken).asserting(_ shouldBe maybeGitUrl)
  }

  it should "return some gitUrl if GL returns 200 with payload containing the 'http_url_to_repo'" in {
    val gitUrl = projectGitHttpUrls.generateOne
    mapResponse(Ok, Request[IO](), Response[IO](Ok).withEntity(gitUrl.asJson(payloadEncoder)))
      .asserting(_ shouldBe Some(gitUrl))
  }

  it should "return no gitUrl if GL returns 404 NOT_FOUND" in {
    mapResponse(NotFound, Request[IO](), Response[IO](NotFound))
      .asserting(_ shouldBe None)
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new ProjectGitUrlFinderImpl[IO]

  private def givenFetchProjectGitUrl(slug:        projects.Slug,
                                      accessToken: AccessToken,
                                      returning:   IO[Option[projects.GitHttpUrl]]
  ) = {
    val endpointName: String Refined NonEmpty = "single-project"
    (glClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[projects.GitHttpUrl]])(
        _: Option[AccessToken]
      ))
      .expects(uri"projects" / slug, endpointName, *, accessToken.some)
      .returning(returning)
  }

  private lazy val mapResponse: ResponseMappingF[IO, Option[projects.GitHttpUrl]] =
    captureMapping(glClient)(
      finder
        .findGitUrl(projectSlugs.generateOne, userAccessTokens.generateOne)
        .unsafeRunSync(),
      projectGitHttpUrls.toGeneratorOfOptions,
      underlyingMethod = Get
    )

  private lazy val payloadEncoder: Encoder[projects.GitHttpUrl] = Encoder.instance { url =>
    Json.obj("http_url_to_repo" -> url.asJson)
  }
}
