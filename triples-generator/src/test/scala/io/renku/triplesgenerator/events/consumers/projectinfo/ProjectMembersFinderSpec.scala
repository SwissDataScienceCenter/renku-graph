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

package io.renku.triplesgenerator.events.consumers.projectinfo

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.http.client.GitLabGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.gitlab.{GitLabMember, GitLabUser}
import io.renku.graph.model.projects
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnexpectedResponseException
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import org.http4s.Status.{BadGateway, Forbidden, ServiceUnavailable, Unauthorized}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectMembersFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with EitherValues
    with GitLabClientTools[IO]
    with MockFactory
    with ScalaCheckPropertyChecks {

  "findProject" should {

    "fetch and merge project users and members" in new TestCase {
      val gitLabMember: Gen[Set[GitLabMember]] = gitLabMemberGen(maybeEmails = Gen.const(None)).toGeneratorOfSet()
      forAll(gitLabMember) { members: Set[GitLabMember] =>
        setGitLabClientExpectation(projectSlug, returning = (members, None).pure[IO])

        finder.findProjectMembers(projectSlug).value.unsafeRunSync().value shouldBe members
      }
    }

    "collect members from all the pages" in new TestCase {

      val members = projectMembersNoEmail.generateFixedSizeSet(ofSize = 4)

      setGitLabClientExpectation(projectSlug, returning = (Set(members.head), 2.some).pure[IO])
      setGitLabClientExpectation(projectSlug, maybePage = 2.some, returning = (members.tail, None).pure[IO])

      finder.findProjectMembers(projectSlug).value.unsafeRunSync().value shouldBe members
    }

    "return an empty set even if GL endpoint responds with NOT_FOUND" in new TestCase {

      setGitLabClientExpectation(projectSlug, returning = (Set.empty[GitLabMember], None).pure[IO])

      finder.findProjectMembers(projectSlug).value.unsafeRunSync().value shouldBe Set.empty
    }

    val errorMessage = nonEmptyStrings().generateOne
    Set(
      "BadGateway"         -> UnexpectedResponseException(BadGateway, errorMessage),
      "ServiceUnavailable" -> UnexpectedResponseException(ServiceUnavailable, errorMessage),
      "Forbidden"          -> UnexpectedResponseException(Forbidden, errorMessage),
      "Unauthorized"       -> UnexpectedResponseException(Unauthorized, errorMessage)
    ) foreach { case (problemName, error) =>
      s"return a Recoverable Failure for $problemName when fetching project members" in new TestCase {

        setGitLabClientExpectation(projectSlug, returning = IO.raiseError(error))

        finder.findProjectMembers(projectSlug).value.unsafeRunSync().left.value shouldBe a[ProcessingRecoverableError]
      }
    }

    // mapResponse

    "return members from Json on multiple pages" in new TestCase {
      val members = projectMembersNoEmail.generateSet(2, 4)

      mapResponse(Status.Ok, Request(), Response().withEntity(Set(members.head.asJson)))
        .unsafeRunSync() shouldBe (Set(members.head), None)
    }

    "return an empty Set if NOT_FOUND" in new TestCase {
      mapResponse(Status.NotFound, Request(), Response()).unsafeRunSync() shouldBe (Set.empty, None)
    }
  }

  private trait TestCase {
    implicit val accessToken: AccessToken = accessTokens.generateOne
    val projectSlug = projectSlugs.generateOne

    private implicit val logger: TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient:   GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new ProjectMembersFinderImpl[IO]

    def setGitLabClientExpectation(projectSlug: projects.Slug,
                                   maybePage:   Option[Int] = None,
                                   returning:   IO[(Set[GitLabMember], Option[Int])]
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
          _: ResponseMappingF[IO, (Set[GitLabMember], Option[Int])]
        )(_: Option[AccessToken]))
        .expects(uri, endpointName, *, accessToken.some)
        .returning(returning)
    }

    val mapResponse =
      captureMapping(gitLabClient)(
        finder.findProjectMembers(projectSlug)(accessToken).value.unsafeRunSync(),
        Gen.const((Set.empty[GitLabMember], Option.empty[Int])),
        underlyingMethod = Get
      )
  }

  private implicit val userEncoder: Encoder[GitLabUser] = Encoder.instance { user =>
    json"""{
          "id":       ${user.gitLabId},
          "name":     ${user.name},
          "username": ${user.username}
        }"""
  }

  private implicit lazy val memberEncoder: Encoder[GitLabMember] = Encoder.instance { member =>
    json"""{
      "id":       ${member.user.gitLabId},
      "name":     ${member.user.name},
      "username": ${member.user.username},
      "access_level": ${member.accessLevel}
    }"""
  }
}
