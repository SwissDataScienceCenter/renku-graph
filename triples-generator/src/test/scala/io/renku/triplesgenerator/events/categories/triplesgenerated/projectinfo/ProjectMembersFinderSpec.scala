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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

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
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.entities.Project.ProjectMember.ProjectMemberNoEmail
import io.renku.graph.model.projects
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnexpectedResponseException
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.http4s.Status.{BadGateway, Forbidden, ServiceUnavailable, Unauthorized}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class ProjectMembersFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with GitLabClientTools[IO]
    with MockFactory
    with ScalaCheckPropertyChecks
    with TinyTypeEncoders {

  "findProject" should {

    "fetch and merge project users and members" in new TestCase {
      forAll { (members: Set[ProjectMemberNoEmail], users: Set[ProjectMemberNoEmail]) =>
        setGitLabClientExpectationUsers(projectPath, returning = (users, None).pure[IO])
        setGitLabClientExpectationMembers(projectPath, returning = (members, None).pure[IO])

        finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe (members ++ users).asRight
      }
    }

    "collect members from all the pages" in new TestCase {
      val allMembers = projectMembersNoEmail.generateFixedSizeList(4)

      val (users, members) = allMembers.splitAt(allMembers.size / 2)

      setGitLabClientExpectationUsers(projectPath, returning = (Set(users.head), 2.some).pure[IO])
      setGitLabClientExpectationUsers(projectPath, maybePage = 2.some, returning = (users.tail.toSet, None).pure[IO])
      setGitLabClientExpectationMembers(projectPath, returning = (Set(members.head), 2.some).pure[IO])
      setGitLabClientExpectationMembers(projectPath,
                                        maybePage = 2.some,
                                        returning = (members.tail.toSet, None).pure[IO]
      )

      finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe allMembers.toSet.asRight
    }

    "return members even if one of the endpoints responds with NOT_FOUND" in new TestCase {
      val members = projectMembersNoEmail.generateSet()
      if (Random.nextBoolean()) {
        setGitLabClientExpectationUsers(projectPath, returning = (Set.empty[ProjectMemberNoEmail], None).pure[IO])
        setGitLabClientExpectationMembers(projectPath, returning = (members, None).pure[IO])
      } else {
        setGitLabClientExpectationUsers(projectPath, returning = (members, None).pure[IO])
        setGitLabClientExpectationMembers(projectPath, returning = (Set.empty[ProjectMemberNoEmail], None).pure[IO])
      }

      finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe members.asRight
    }

    val errorMessage = nonEmptyStrings().generateOne
    Set(
      "BadGateway"         -> UnexpectedResponseException(BadGateway, errorMessage),
      "ServiceUnavailable" -> UnexpectedResponseException(ServiceUnavailable, errorMessage),
      "Forbidden"          -> UnexpectedResponseException(Forbidden, errorMessage),
      "Unauthorized"       -> UnexpectedResponseException(Unauthorized, errorMessage)
    ) foreach { case (problemName, error) =>
      s"return a Recoverable Failure for $problemName when fetching project members or users" in new TestCase {
        if (Random.nextBoolean()) {
          setGitLabClientExpectationUsers(projectPath, returning = (Set.empty[ProjectMemberNoEmail], None).pure[IO])
          setGitLabClientExpectationMembers(projectPath, returning = IO.raiseError(error))
        } else {
          setGitLabClientExpectationUsers(projectPath, returning = IO.raiseError(error))
          setGitLabClientExpectationMembers(projectPath, returning = (Set.empty[ProjectMemberNoEmail], None).pure[IO])
        }

        val Left(failure) = finder.findProjectMembers(projectPath).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
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
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val projectPath = projectPaths.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabClient = mock[GitLabClient[IO]]
    val finder       = new ProjectMembersFinderImpl[IO](gitLabClient)

    def setGitLabClientExpectationUsers(projectPath: projects.Path,
                                        maybePage:   Option[Int] = None,
                                        returning:   IO[(Set[ProjectMemberNoEmail], Option[Int])]
    ) = setGitLabClientExpectation("users", projectPath, maybePage, returning)

    def setGitLabClientExpectationMembers(projectPath: projects.Path,
                                          maybePage:   Option[Int] = None,
                                          returning:   IO[(Set[ProjectMemberNoEmail], Option[Int])]
    ) = setGitLabClientExpectation("members", projectPath, maybePage, returning)

    private def setGitLabClientExpectation(endpointName: String Refined NonEmpty,
                                           projectPath:  projects.Path,
                                           maybePage:    Option[Int] = None,
                                           returning:    IO[(Set[ProjectMemberNoEmail], Option[Int])]
    ) = {

      val uri = {
        val uri = uri"projects" / projectPath.show / endpointName
        maybePage match {
          case Some(page) => uri withQueryParam ("page", page.toString)
          case None       => uri
        }
      }

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(
          _: ResponseMappingF[IO, (Set[ProjectMemberNoEmail], Option[Int])]
        )(_: Option[AccessToken]))
        .expects(uri, endpointName, *, maybeAccessToken)
        .returning(returning)
    }

    val mapResponse =
      captureMapping(finder, gitLabClient)(
        _.findProjectMembers(projectPath)(maybeAccessToken).value.unsafeRunSync(),
        Gen.const((Set.empty[ProjectMemberNoEmail], Option.empty[Int])),
        expectedNumberOfCalls = 2
      )
  }

  private implicit lazy val memberEncoder: Encoder[ProjectMemberNoEmail] = Encoder.instance { member =>
    json"""{
      "id":       ${member.gitLabId},     
      "name":     ${member.name},
      "username": ${member.username}
    }"""
  }

}
