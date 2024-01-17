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

import BranchProtectionCheck.BranchInfo
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.core.client.Generators.branches
import io.renku.http.client.GitLabGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.booleans
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.GitLabClientTools
import org.http4s.Status.{NotFound, Ok}
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class BranchProtectionCheckSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues
    with RenkuEntityCodec
    with GitLabClientTools[IO] {

  it should "call GL's GET gl/projects/:slug/repository/branches and return Unprotected " +
    "if the default branch has can_push=true" in {

      val slug          = projectSlugs.generateOne
      val accessToken   = accessTokens.generateOne
      val defaultBranch = branches.generateOne

      givenProjectBranches(
        slug,
        accessToken,
        returning = List(
          BranchInfo(branches.generateOne, default = false, canPush = true),
          BranchInfo(branches.generateOne, default = false, canPush = false),
          BranchInfo(defaultBranch, default = true, canPush = true)
        ).pure[IO]
      )

      finder
        .findDefaultBranchInfo(slug, accessToken)
        .asserting(_ shouldBe DefaultBranch.Unprotected(defaultBranch).some)
    }

  it should "call GL's GET gl/projects/:slug/repository/branches and return PushProtected " +
    "if the default branch has can_push=false" in {

      val slug          = projectSlugs.generateOne
      val accessToken   = accessTokens.generateOne
      val defaultBranch = branches.generateOne

      givenProjectBranches(
        slug,
        accessToken,
        returning = List(
          BranchInfo(branches.generateOne, default = false, canPush = true),
          BranchInfo(branches.generateOne, default = false, canPush = false),
          BranchInfo(defaultBranch, default = true, canPush = false)
        ).pure[IO]
      )

      finder
        .findDefaultBranchInfo(slug, accessToken)
        .asserting(_ shouldBe DefaultBranch.PushProtected(defaultBranch).some)
    }

  it should "call GL's GET gl/projects/:slug/repository/branches and return false " +
    "if there's no default branch" in {

      val slug        = projectSlugs.generateOne
      val accessToken = accessTokens.generateOne

      givenProjectBranches(
        slug,
        accessToken,
        returning = List(BranchInfo(branches.generateOne, default = false, canPush = true),
                         BranchInfo(branches.generateOne, default = false, canPush = false)
        ).pure[IO]
      )

      finder.findDefaultBranchInfo(slug, accessToken).asserting(_ shouldBe None)
    }

  it should "call GL's GET gl/projects/:slug/repository/branches and return false " +
    "if there are no branches found" in {

      val slug        = projectSlugs.generateOne
      val accessToken = accessTokens.generateOne

      givenProjectBranches(
        slug,
        accessToken,
        returning = List.empty.pure[IO]
      )

      finder.findDefaultBranchInfo(slug, accessToken).asserting(_ shouldBe None)
    }

  it should "return a list branches if GL returns 200 with a non empty list" in {
    val branches = branchInfos.generateList(min = 1)
    mapResponse(Ok, Request[IO](), Response[IO](Ok).withEntity(branches.asJson))
      .asserting(_ shouldBe branches)
  }

  it should "return an empty list if GL returns 200 with an empty list of branches" in {
    mapResponse(Ok, Request[IO](), Response[IO](Ok).withEntity(List.empty[BranchInfo].asJson))
      .asserting(_ shouldBe Nil)
  }

  it should "return an empty list if GL returns 404 NOT_FOUND" in {
    mapResponse(NotFound, Request[IO](), Response[IO](NotFound))
      .asserting(_ shouldBe Nil)
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new BranchProtectionCheckImpl[IO]

  private def givenProjectBranches(slug: projects.Slug, accessToken: AccessToken, returning: IO[List[BranchInfo]]) = {
    val endpointName: String Refined NonEmpty = "project-branches"
    (glClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, List[BranchInfo]])(_: Option[AccessToken]))
      .expects(uri"projects" / slug / "repository" / "branches", endpointName, *, accessToken.some)
      .returning(returning)
  }

  private lazy val mapResponse: ResponseMappingF[IO, List[BranchInfo]] =
    captureMapping(glClient)(
      finder
        .findDefaultBranchInfo(projectSlugs.generateOne, accessTokens.generateOne)
        .unsafeRunSync(),
      branchInfos.toGeneratorOfList(),
      underlyingMethod = Get
    )

  private implicit lazy val itemEncoder: Encoder[BranchInfo] = Encoder.instance {
    case BranchInfo(name, default, canPush) => json"""{
      "name":     $name,
      "can_push": $canPush,
      "default":  $default
    }"""
  }

  private lazy val branchInfos: Gen[BranchInfo] =
    (branches, booleans, booleans).mapN(BranchInfo.apply)
}
