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

package io.renku.tokenrepository.repository.creation

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.http.client.GitLabGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{personGitLabIds, projectIds}
import io.renku.graph.model.projects.Role
import io.renku.graph.model.{persons, projects}
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Status.{BadRequest, Forbidden, NotFound, Ok, Unauthorized}
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class TokenValidatorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "checkValid" should {

    "return true if the user the token belongs to " +
      "is a member of the project with the given projectId " +
      "and it has sufficient rights to it" in new TestCase {

        val accessToken = accessTokens.generateOne
        val user        = personGitLabIds.generateOne
        givenFindingUser(accessToken, returning = user.some)

        val projectId = projectIds.generateOne
        givenFindingRights(user, projectId, accessToken, returning = true)

        validator.checkValid(projectId, accessToken).unsafeRunSync() shouldBe true
      }

    "return false if the user the token belongs to " +
      "is a member of the project with the given projectId " +
      "but it has insufficient rights to it" in new TestCase {

        val accessToken = accessTokens.generateOne
        val user        = personGitLabIds.generateOne
        givenFindingUser(accessToken, returning = user.some)

        val projectId = projectIds.generateOne
        givenFindingRights(user, project = projectId, accessToken, returning = false)

        validator.checkValid(projectId, accessToken).unsafeRunSync() shouldBe false
      }

    "return false if the token is invalid" in new TestCase {

      val accessToken = accessTokens.generateOne
      givenFindingUser(accessToken, returning = None)

      val projectId = projectIds.generateOne

      validator.checkValid(projectId, accessToken).unsafeRunSync() shouldBe false
    }
  }

  private trait TestCase {
    private val userIdFinder        = mock[UserIdFinder[IO]]
    private val memberRightsChecker = mock[MemberRightsChecker[IO]]
    val validator                   = new TokenValidatorImpl[IO](userIdFinder, memberRightsChecker)

    def givenFindingUser(accessToken: AccessToken, returning: Option[persons.GitLabId]) =
      (userIdFinder.findUserId _)
        .expects(accessToken)
        .returning(returning.pure[IO])

    def givenFindingRights(user:        persons.GitLabId,
                           project:     projects.GitLabId,
                           accessToken: AccessToken,
                           returning:   Boolean
    ) = (memberRightsChecker.checkRights _)
      .expects(project, user, accessToken)
      .returning(returning.pure[IO])
  }
}

class UserIdFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO]
    with TableDrivenPropertyChecks
    with should.Matchers
    with RenkuEntityCodec {

  "checkValid" should {

    "return userId if returned" in new TestCase {

      val accessToken = accessTokens.generateOne
      val maybeUserId = personGitLabIds.generateOption
      givenFindingUser(accessToken, returning = maybeUserId)

      finder.findUserId(accessToken).unsafeRunSync() shouldBe maybeUserId
    }

    forAll {
      Table(
        ("Case", "Response", "Expected Result"),
        ("ok valid", Response[IO](Ok).withEntity(json"""{"id": ${persons.GitLabId(1)}}"""), Some(persons.GitLabId(1))),
        ("ok invalid", Response[IO](Ok).withEntity(json"""{}"""), None),
        ("unauthorized", Response[IO](Unauthorized), None),
        ("forbidden", Response[IO](Forbidden), None),
        ("notFound", Response[IO](NotFound), None)
      )
    } { (caze, response, result) =>
      show"map user API call responding $caze to $result" in new TestCase {
        responseMapping(response.status, Request[IO](), response).unsafeRunSync() shouldBe result
      }
    }

    "throw an Exception if remote responds with status different than OK, NOT_FOUND or UNAUTHORIZED" in new TestCase {
      intercept[Exception] {
        responseMapping(BadRequest, Request[IO](), Response[IO](BadRequest)).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new UserIdFinderImpl[IO]

    private val userEndpointName: String Refined NonEmpty = "user"

    lazy val responseMapping = captureMapping(gitLabClient)(
      findingMethod = finder.findUserId(accessTokens.generateOne).unsafeRunSync(),
      resultGenerator = personGitLabIds.generateOption,
      maybeEndpointName = userEndpointName.some,
      underlyingMethod = Get
    )

    def givenFindingUser(accessToken: AccessToken, returning: Option[persons.GitLabId]) =
      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[persons.GitLabId]])(
          _: Option[AccessToken]
        ))
        .expects(uri"user", userEndpointName, *, accessToken.some)
        .returning(returning.pure[IO])
  }
}

class MemberRightsCheckerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO]
    with TableDrivenPropertyChecks
    with should.Matchers
    with RenkuEntityCodec {

  def accessLevel(r: Role): Int = Role.toGitLabAccessLevel(r)

  "checkValid" should {

    "return boolean based on the response from the GET to GL's project member API" in new TestCase {

      val accessToken = accessTokens.generateOne
      val userId      = personGitLabIds.generateOne
      val projectId   = projectIds.generateOne
      val result      = results.generateOne
      givenCheckingMemberRights(userId, projectId, accessToken, returning = result)

      rightsChecker.checkRights(projectId, userId, accessToken).unsafeRunSync() shouldBe result
    }

    forAll {
      Table(
        ("Case", "Response", "Expected Result"),
        ("ok role 30 and active",
         Response[IO](Ok).withEntity(json"""{"access_level": ${accessLevel(Role.Reader)}, "state": "active"}"""),
         false
        ),
        ("ok role 40 and active",
         Response[IO](Ok).withEntity(json"""{"access_level": ${accessLevel(Role.Maintainer)}, "state": "active"}"""),
         true
        ),
        ("ok role 40 and non-active",
         Response[IO](Ok).withEntity(json"""{"access_level": ${accessLevel(Role.Owner)}, "state": "waiting"}"""),
         false
        ),
        ("ok role 40 and no state",
         Response[IO](Ok).withEntity(json"""{"access_level": ${accessLevel(Role.Owner)}}"""),
         false
        ),
        ("ok role 50 and active",
         Response[IO](Ok).withEntity(json"""{"access_level": ${accessLevel(Role.Owner)}, "state": "active"}"""),
         true
        ),
        ("ok invalid", Response[IO](Ok).withEntity(json"""{}"""), false),
        ("unauthorized", Response[IO](Unauthorized), false),
        ("forbidden", Response[IO](Forbidden), false),
        ("notFound", Response[IO](NotFound), false)
      )
    } { (caze, response, result) =>
      show"map project member API call responding $caze to $result" in new TestCase {
        responseMapping(response.status, Request[IO](), response).unsafeRunSync() shouldBe result
      }
    }

    "throw an Exception if remote responds with status different than OK, NOT_FOUND or UNAUTHORIZED" in new TestCase {
      intercept[Exception] {
        responseMapping(BadRequest, Request[IO](), Response[IO](BadRequest)).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val rightsChecker = new MemberRightsCheckerImpl[IO]

    private val projectMemberEndpointName: String Refined NonEmpty = "single-project-member"

    lazy val responseMapping = captureMapping(gitLabClient)(
      findingMethod = rightsChecker
        .checkRights(projectIds.generateOne, personGitLabIds.generateOne, accessTokens.generateOne)
        .unsafeRunSync(),
      resultGenerator = results.generateOne,
      maybeEndpointName = projectMemberEndpointName.some,
      underlyingMethod = Get
    )

    def givenCheckingMemberRights(user:        persons.GitLabId,
                                  project:     projects.GitLabId,
                                  accessToken: AccessToken,
                                  returning:   Boolean
    ) = (gitLabClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Boolean])(_: Option[AccessToken]))
      .expects(uri"projects" / project / "members" / "all" / user, projectMemberEndpointName, *, accessToken.some)
      .returning(returning.pure[IO])
  }

  private lazy val results = Gen.oneOf(true, false)
}
