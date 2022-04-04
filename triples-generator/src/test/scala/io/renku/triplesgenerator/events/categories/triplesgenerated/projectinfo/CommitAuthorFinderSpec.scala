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

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.{accessTokens, clientExceptions, connectivityExceptions, unexpectedResponseExceptions}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonBlankStrings
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.persons
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.http4s.Method.GET
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Method, Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CommitAuthorFinderSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with MockFactory
    with ScalaCheckPropertyChecks
    with TinyTypeEncoders
    with GitLabClientTools[IO] {

  "findCommitAuthor" should {

    "return the result from the GitLabClient" in new TestCase {
      forAll { (authorName: persons.Name, authorEmail: persons.Email) =>
        val expectation = (authorName -> authorEmail).some
        val endpointName: String Refined NonEmpty = "commit-detail"

        (gitLabClient
          .send(_: Method, _: Uri, _: String Refined NonEmpty)(
            _: ResponseMappingF[IO, Option[(persons.Name, persons.Email)]]
          )(_: Option[AccessToken]))
          .expects(GET,
                   uri"projects" / projectPath.show / "repository" / "commits" / commitId.show,
                   endpointName,
                   *,
                   maybeAccessToken
          )
          .returning(expectation.pure[IO])

        finder
          .findCommitAuthor(projectPath, commitId)
          .value
          .unsafeRunSync() shouldBe (authorName -> authorEmail).some.asRight
      }
    }

    "return a ProcessingRecoverableError if the GitLabClient " +
      "throws an exception handled by the recovery strategy" in new TestCase {

        val endpointName: String Refined NonEmpty = "commit-detail"

        val error = Gen.oneOf(clientExceptions, connectivityExceptions, unexpectedResponseExceptions).generateOne

        (gitLabClient
          .send(_: Method, _: Uri, _: String Refined NonEmpty)(
            _: ResponseMappingF[IO, Option[(persons.Name, persons.Email)]]
          )(_: Option[AccessToken]))
          .expects(GET,
                   uri"projects" / projectPath.show / "repository" / "commits" / commitId.show,
                   endpointName,
                   *,
                   maybeAccessToken
          )
          .returning(error.raiseError[IO, Option[(persons.Name, persons.Email)]])

        val Left(result) = finder
          .findCommitAuthor(projectPath, commitId)
          .value
          .unsafeRunSync()
        result            shouldBe a[ProcessingRecoverableError]
        result.getMessage shouldBe error.getMessage
      }

    "return commit author's name and email if found" in new TestCase {
      forAll { (authorName: persons.Name, authorEmail: persons.Email) =>
        mapResponse(Status.Ok, Request[IO](), Response[IO]().withEntity((authorName -> authorEmail).asJson.noSpaces))
          .unsafeRunSync() shouldBe
          (authorName            -> authorEmail).some
      }
    }

    "return None if commit NOT_FOUND" in new TestCase {
      mapResponse(Status.NotFound, Request[IO](), Response[IO]()).unsafeRunSync() shouldBe None
    }

    "return None if commit author not found" in new TestCase {
      mapResponse(Status.Ok, Request[IO](), Response[IO]().withEntity(json"""{}""")).unsafeRunSync() shouldBe None
    }

    "return None if commit author email invalid" in new TestCase {
      mapResponse(
        Status.Ok,
        Request[IO](),
        Response[IO]().withEntity(
          json"""{ "author_name":  ${personNames.generateOne}, "author_email": ${nonBlankStrings().generateOne.value} }""".noSpaces
        )
      ).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val projectPath = projectPaths.generateOne
    val commitId    = commitIds.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabClient = mock[GitLabClient[IO]]
    val finder       = new CommitAuthorFinderImpl[IO](gitLabClient)

    val mapResponse =
      captureMapping(finder, gitLabClient)(
        _.findCommitAuthor(projectPaths.generateOne, commitIds.generateOne),
        Gen.const(EitherT(IO(Option.empty[(persons.Name, persons.Email)].asRight[ProcessingRecoverableError])))
      )

  }

  private implicit lazy val authorEncoder: Encoder[(persons.Name, persons.Email)] = Encoder.instance {
    case (authorName, authorEmail) => json"""{
      "author_name":  $authorName,
      "author_email": $authorEmail
    }"""
  }
}
