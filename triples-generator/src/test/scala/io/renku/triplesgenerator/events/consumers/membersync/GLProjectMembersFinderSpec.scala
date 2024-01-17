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

package io.renku.triplesgenerator.events.consumers.membersync

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._
import io.renku.http.client.GitLabGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.GitLabClientTools
import org.http4s.Status.{Forbidden, Unauthorized}
import org.http4s.implicits._
import org.http4s.{Header, Headers, Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.ci._

class GLProjectMembersFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with ExternalServiceStubbing
    with AsyncMockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with RenkuEntityCodec
    with GitLabClientTools[IO] {

  private implicit val accessToken: AccessToken = accessTokens.generateOne

  it should "return a set of all project members" in {

    val slug           = projectSlugs.generateOne
    val projectMembers = gitLabProjectMembers.generateSet()

    setGitLabClientExpectation(slug, None, returning = (projectMembers, None))

    finder.findProjectMembers(slug).asserting(_ shouldBe projectMembers)
  }

  it should "collect members from all pages of results" in {

    val slug           = projectSlugs.generateOne
    val projectMembers = gitLabProjectMembers.generateNonEmptyList(min = 2).toList.toSet

    setGitLabClientExpectation(slug, None, returning = (Set(projectMembers.head), 2.some))
    setGitLabClientExpectation(slug, 2.some, returning = (projectMembers.tail, None))

    finder.findProjectMembers(slug).asserting(_ shouldBe projectMembers)
  }

  it should "parse results and request next page" in {

    val members = gitLabProjectMembers.generateNonEmptyList(min = 2).toList.toSet

    val nextPage   = 2
    val totalPages = 2
    val headers = Headers(
      List(Header.Raw(ci"X-Next-Page", nextPage.toString), Header.Raw(ci"X-Total-Pages", totalPages.toString))
    )

    mapResponse(Status.Ok, Request[IO](), Response[IO]().withEntity(members.asJson).withHeaders(headers))
      .asserting(_ shouldBe (members, Some(2)))
  }

  it should "skip items without an id or/and name" in {

    val members = gitLabProjectMembers.generateNonEmptyList().toList.toSet

    val memberWithoutIdAndName = json"""{"access_level": 40}"""

    mapResponse(Status.Ok,
                Request[IO](),
                Response().withEntity(Json.arr(memberWithoutIdAndName :: members.map(_.asJson).toList: _*))
    ).asserting(_ shouldBe (members, None))
  }

  it should "return an empty set when service responds with NOT_FOUND" in {
    mapResponse(Status.NotFound, Request(), Response())
      .asserting(_ shouldBe (Set.empty[GitLabProjectMember], None))
  }

  Forbidden +: Unauthorized +: Nil foreach { status =>
    it should s"return an empty set when service responds with $status" in {

      val at   = accessTokens.generateOne
      val slug = projectSlugs.generateOne

      val mapResponse =
        captureMapping(gitLabClient)(
          finder.findProjectMembers(slug)(at).unsafeRunSync(),
          Gen.const((Set.empty[GitLabProjectMember], Option.empty[Int])),
          underlyingMethod = Get
        )

      mapResponse(status, Request(), Response()).asserting(_ shouldBe (Set.empty[GitLabProjectMember], None))
    }
  }

  private implicit val logger:    TestLogger[IO]   = TestLogger[IO]()
  implicit lazy val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new GLProjectMembersFinderImpl[IO]

  private def setGitLabClientExpectation(projectSlug: projects.Slug,
                                         maybePage:   Option[Int] = None,
                                         returning:   (Set[GitLabProjectMember], Option[Int])
  ) = {
    val endpointName: String Refined NonEmpty = "project-members"

    val uri = {
      val uri = uri"projects" / projectSlug / "members" / "all"
      maybePage match {
        case Some(page) => uri withQueryParam ("page", page.toString)
        case None       => uri
      }
    }

    (gitLabClient
      .get(_: Uri, _: String Refined NonEmpty)(
        _: ResponseMappingF[IO, (Set[GitLabProjectMember], Option[Int])]
      )(_: Option[AccessToken]))
      .expects(uri, endpointName, *, accessToken.some)
      .returning(returning.pure[IO])
  }

  private lazy val mapResponse =
    captureMapping(gitLabClient)(
      finder.findProjectMembers(projectSlugs.generateOne)(accessToken).unsafeRunSync(),
      Gen.const((Set.empty[GitLabProjectMember], Option.empty[Int])),
      underlyingMethod = Get
    )

  private implicit val projectMemberEncoder: Encoder[GitLabProjectMember] = Encoder.instance[GitLabProjectMember] {
    member =>
      json"""{
        "id":           ${member.gitLabId.value},
        "username":     ${member.name.value},
        "name":         ${member.name.value},
        "access_level": ${member.accessLevel}
      }"""
  }
}
