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

package io.renku.triplesgenerator.events.consumers.projectinfo

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
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.gitlab.{GitLabMember, GitLabProjectInfo, GitLabUser}
import io.renku.graph.model.projects.Slug
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.json.JsonOps._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import ProcessingRecoverableError._
import org.http4s.Status.{BadGateway, Forbidden, ServiceUnavailable, Unauthorized}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectFinderSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with EitherValues
    with MockFactory
    with ScalaCheckPropertyChecks
    with GitLabClientTools[IO] {

  "findProject" should {

    "fetch info about the project with the given slug from GitLab" in new TestCase {
      implicit val gitLabUser: Gen[GitLabUser] = gitLabUserGen(maybeEmails = Gen.const(None))
      forAll { (projectInfoRaw: GitLabProjectInfo, creator: GitLabUser) =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = creator.some, members = Set.empty)

        setGitLabClientExpectationProjects(projectInfo.slug, (projectInfo, creator.gitLabId.some).some.pure[IO])
        setGitLabClientExpectationUsers(creator.gitLabId, creator.some.pure[IO])

        finder.findProject(projectInfo.slug).value.unsafeRunSync() shouldBe
          projectInfo.copy(members = Set.empty, maybeCreator = creator.some).some.asRight
      }
    }

    "fetch info about the project without creator if it does not exist" in new TestCase {
      implicit val gitLabUser: Gen[GitLabUser] = gitLabUserGen(maybeEmails = Gen.const(None))
      forAll { (projectInfoRaw: GitLabProjectInfo, creator: GitLabUser) =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = creator.some, members = Set.empty)

        setGitLabClientExpectationProjects(projectInfo.slug, (projectInfo, creator.gitLabId.some).some.pure[IO])
        setGitLabClientExpectationUsers(creator.gitLabId, None.pure[IO])

        finder.findProject(projectInfo.slug).value.unsafeRunSync() shouldBe
          projectInfo.copy(maybeCreator = None, members = Set.empty).some.asRight
      }
    }

    val shouldBeLogWorthy = (failure: ProcessingRecoverableError) => failure shouldBe a[LogWorthyRecoverableError]
    val shouldBeSilent    = (failure: ProcessingRecoverableError) => failure shouldBe a[SilentRecoverableError]
    val errorMessage      = nonEmptyStrings().generateOne

    forAll(
      Table(
        ("Problem Name", "Failing Response", "Expected Failure type"),
        ("connection problem", ConnectivityException(errorMessage, exceptions.generateOne), shouldBeLogWorthy),
        ("client problem", ClientException(errorMessage, exceptions.generateOne), shouldBeLogWorthy),
        ("BadGateway", UnexpectedResponseException(BadGateway, errorMessage), shouldBeLogWorthy),
        ("ServiceUnavailable", UnexpectedResponseException(ServiceUnavailable, errorMessage), shouldBeLogWorthy),
        ("Forbidden", UnexpectedResponseException(Forbidden, errorMessage), shouldBeSilent),
        ("Unauthorized", UnexpectedResponseException(Unauthorized, errorMessage), shouldBeSilent)
      )
    ) { case (problemName, error, failureTypeAssertion) =>
      s"return a Recoverable Failure for $problemName when fetching project info" in new TestCase {
        val slug = projectSlugs.generateOne

        setGitLabClientExpectationProjects(slug, IO.raiseError(error))

        val Left(failure) = finder.findProject(slug).value.unsafeRunSync()
        failureTypeAssertion(failure)
      }

      s"return a Recoverable Failure for $problemName when fetching creator" in new TestCase {
        val creator     = projectMembersNoEmail.generateOne
        val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = creator.user.some)

        setGitLabClientExpectationProjects(projectInfo.slug, IO.raiseError(error))

        val failure = finder.findProject(projectInfo.slug).value.unsafeRunSync().left.value
        failureTypeAssertion(failure)
      }
    }

    // mapTo tests

    "default to visibility Public if not returned (quite likely due to invalid token)" in new TestCase {
      // It should be safe as for non-public repos and invalid/no token we'd not get any response
      val projectInfo = gitLabProjectInfos.generateOne
        .copy(maybeCreator = None, visibility = projects.Visibility.Public, members = Set.empty)

      val json = projectInfo.asJson.hcursor
        .downField("visibility")
        .delete
        .top
        .getOrElse(fail("Deleting visibility failed"))

      mapTo(Status.Ok, Request(), Response().withEntity(json.noSpaces)).unsafeRunSync() shouldBe
        (projectInfo.copy(maybeCreator = None, members = Set.empty), None).some
    }

    "return info without creator if it's not returned from the GET projects/:id" in new TestCase {
      forAll { projectInfoRaw: GitLabProjectInfo =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = None, members = Set.empty)

        val result = mapTo(Status.Ok, Request(), Response().withEntity(projectInfo.asJson.noSpaces)).unsafeRunSync()
        result shouldBe
          (projectInfo.copy(maybeCreator = None, members = Set.empty, avatarUrl = projectInfo.avatarUrl), None).some
      }
    }

    "return no info when there's no project with the given slug" in new TestCase {
      mapTo(Status.NotFound, Request(), Response()).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient:   GitLabClient[IO] = mock[GitLabClient[IO]]

    val finder = new ProjectFinderImpl[IO]

    private type ProjectAndCreators = (GitLabProjectInfo, Option[persons.GitLabId])

    def setGitLabClientExpectationUsers(id: persons.GitLabId, returning: IO[Option[GitLabUser]]) =
      setGitLabClientExpectation("single-user", id.show, returning)

    def setGitLabClientExpectationProjects(id: projects.Slug, returning: IO[Option[ProjectAndCreators]]) =
      setGitLabClientExpectation("single-project", id.show, returning)

    private def setGitLabClientExpectation[ResultType](
        endpointName: String Refined NonEmpty,
        id:           String,
        returning:    IO[ResultType]
    ) = {
      val endpointStart = if (endpointName.value == "single-project") uri"projects" else uri"users"
      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(
          _: ResponseMappingF[IO, ResultType]
        )(_: Option[AccessToken]))
        .expects(endpointStart / id, endpointName, *, maybeAccessToken)
        .returning(returning)
    }

    val mapTo: ResponseMappingF[IO, Option[(GitLabProjectInfo, Option[persons.GitLabId])]] =
      captureMapping(gitLabClient)(
        finder.findProject(projectSlugs.generateOne)(maybeAccessToken).value.unsafeRunSync(),
        Gen.const((gitLabProjectInfos.generateOne, Option.empty[persons.GitLabId]).some),
        underlyingMethod = Get
      )
  }

  private implicit lazy val projectInfoEncoder: Encoder[GitLabProjectInfo] = Encoder.instance { project =>
    val parenSlugEncoder: Encoder[Slug] = Encoder.instance(slug => json"""{
      "path_with_namespace": $slug
    }""")

    json"""{
      "id":                  ${project.id},
      "path_with_namespace": ${project.slug},
      "name":                ${project.name},
      "created_at":          ${project.dateCreated},
      "created_at":          ${project.dateCreated},
      "updated_at":          ${project.dateModified},
      "visibility":          ${project.visibility},
      "topics":              ${project.keywords.map(_.value) + blankStrings().generateOne}
    }"""
      .addIfDefined("forked_from_project" -> project.maybeParentSlug)(parenSlugEncoder)
      .addIfDefined("creator_id" -> project.maybeCreator.map(_.gitLabId))
      .addIfDefined("description" -> project.maybeDescription.map(_.value))
      .addIfDefined("avatar_url" -> project.avatarUrl)
  }

  private implicit lazy val userEncoder: Encoder[GitLabUser] = Encoder.instance { user =>
    json"""{
      "id":       ${user.gitLabId},
      "name":     ${user.name},
      "username": ${user.username}
    }"""
  }

  private implicit lazy val memberEncoder: Encoder[GitLabMember] =
    Encoder.instance { member =>
      json"""{
          "id":       ${member.user.gitLabId},
          "name":     ${member.user.name},
          "username": ${member.user.username},
          "access_level": ${member.accessLevel}
        }"""
    }
}
