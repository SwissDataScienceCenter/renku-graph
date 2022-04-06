/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.membersync

/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester.jsonEntityEncoder
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.triplesgenerator.events.categories.membersync.Generators._
import org.http4s.Status.{Forbidden, Unauthorized}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Header, Headers, Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.ci.CIStringSyntax

class GitLabProjectMembersFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with GitLabClientTools[IO] {

  "findProjectMembers" should {

    "return a set of project members and users" in new TestCase {

      forAll { (gitLabProjectUsers: Set[GitLabProjectMember], gitLabProjectMembers: Set[GitLabProjectMember]) =>
        setGitLabClientExpectation("users", path, None, returning = (gitLabProjectUsers, None))
        setGitLabClientExpectation("members", path, None, returning = (gitLabProjectMembers, None))

        finder.findProjectMembers(path).unsafeRunSync() shouldBe (gitLabProjectUsers ++ gitLabProjectMembers)
      }
    }

    "collect users from paged results" in new TestCase {
      val projectUsers   = gitLabProjectMembers.generateNonEmptyList(minElements = 2).toList.toSet
      val projectMembers = gitLabProjectMembers.generateNonEmptyList(minElements = 2).toList.toSet

      setGitLabClientExpectation("users", path, None, returning = (Set(projectUsers.head), 2.some))
      setGitLabClientExpectation("users", path, 2.some, returning = (projectUsers.tail, None))
      setGitLabClientExpectation("members", path, None, returning = (Set(projectMembers.head), 2.some))
      setGitLabClientExpectation("members", path, 2.some, returning = (projectMembers.tail, None))

      finder.findProjectMembers(path).unsafeRunSync() shouldBe (projectUsers ++ projectMembers)
    }

    // test map response

    "parse results and request next page" in new TestCase {
      val projectUsers = gitLabProjectMembers.generateNonEmptyList(minElements = 2).toList.toSet

      val nextPage   = 2
      val totalPages = 2
      val headers = Headers(
        List(Header.Raw(ci"X-Next-Page", nextPage.toString()), Header.Raw(ci"X-Total-Pages", totalPages.toString()))
      )

      mapResponse(Status.Ok, Request(), Response().withEntity(projectUsers.asJson).withHeaders(headers))
        .unsafeRunSync() shouldBe (projectUsers, Some(2))
    }

    "return an empty set when service responds with NOT_FOUND" in new TestCase {

      mapResponse(Status.NotFound, Request(), Response()).unsafeRunSync() shouldBe (Set
        .empty[GitLabProjectMember], None)
    }

    Forbidden +: Unauthorized +: Nil foreach { status =>
      s"try without an access token when service responds with $status" in new TestCase {
        val members = gitLabProjectMembers.generateNonEmptyList().toList.toSet

        implicit override val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome

        override val mapResponse =
          captureMapping(finder, gitLabClient)(
            _.findProjectMembers(path)(maybeAccessToken).unsafeRunSync(),
            Gen.const((Set.empty[GitLabProjectMember], Option.empty[Int])),
            expectedNumberOfCalls = 2
          )

        setGitLabClientExpectation("members",
                                   path,
                                   maybePage = None,
                                   maybeAccessTokenOverride = None,
                                   returning = (members, None)
        )

        mapResponse(status, Request(), Response()).unsafeRunSync() shouldBe (members, None)

      }

      s"return an empty set when service responds with $status without access token" in new TestCase {
        implicit override val maybeAccessToken: Option[AccessToken] = Option.empty[AccessToken]

        override val mapResponse =
          captureMapping(finder, gitLabClient)(
            _.findProjectMembers(path)(maybeAccessToken).unsafeRunSync(),
            Gen.const((Set.empty[GitLabProjectMember], Option.empty[Int])),
            expectedNumberOfCalls = 2
          )

        val actual   = mapResponse(status, Request(), Response()).unsafeRunSync()
        val expected = (Set.empty[GitLabProjectMember], None)
        actual shouldBe expected
      }
    }
  }

  private trait TestCase {

    val path = projectPaths.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabClient = mock[GitLabClient[IO]]
    val finder       = new GitLabProjectMembersFinderImpl[IO](gitLabClient)

    def setGitLabClientExpectation(endpointName:             String Refined NonEmpty,
                                   projectPath:              projects.Path,
                                   maybePage:                Option[Int] = None,
                                   maybeAccessTokenOverride: Option[AccessToken] = maybeAccessToken,
                                   returning:                (Set[GitLabProjectMember], Option[Int])
    ) = {

      val uri = {
        val uri = uri"projects" / urlEncode(projectPath.value) / endpointName
        maybePage match {
          case Some(page) => uri withQueryParam ("page", page.toString)
          case None       => uri
        }
      }

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(
          _: ResponseMappingF[IO, (Set[GitLabProjectMember], Option[Int])]
        )(_: Option[AccessToken]))
        .expects(uri, endpointName, *, maybeAccessTokenOverride)
        .returning(returning.pure[IO])
    }

    val mapResponse =
      captureMapping(finder, gitLabClient)(
        _.findProjectMembers(path)(maybeAccessToken).unsafeRunSync(),
        Gen.const((Set.empty[GitLabProjectMember], Option.empty[Int])),
        expectedNumberOfCalls = 2
      )

  }

  private implicit val projectMemberEncoder: Encoder[GitLabProjectMember] = Encoder.instance[GitLabProjectMember] {
    member =>
      json"""{
        "id":        ${member.gitLabId.value},
        "username":  ${member.name.value},
        "name":      ${member.name.value}
      }"""
  }
}
