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

package io.renku.knowledgegraph.users.projects.finder

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{personGitLabIds, personNames}
import io.renku.graph.model.persons
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GLCreatorFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with ExternalServiceStubbing
    with MockFactory
    with IOSpec
    with GitLabClientTools[IO] {

  "findCreatorName" should {

    "call the GitLab Single User API and return the name" in new TestCase {

      val maybeCreatorName = personNames.generateOption

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(
          _: ResponseMappingF[IO, Option[persons.Name]]
        )(_: Option[AccessToken]))
        .expects(uri(creatorId), endpointName, *, maybeAccessToken)
        .returning(maybeCreatorName.pure[IO])

      finder.findCreatorName(creatorId).unsafeRunSync() shouldBe maybeCreatorName
    }

    "map OK response from GitLab to Name" in new TestCase {
      val name = personNames.generateOne
      mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(json"""{"name":  ${name.value}}"""))
        .unsafeRunSync() shouldBe Some(name)
    }

    "map NOT_FOUND to None" in new TestCase {
      mapResponse(Status.NotFound, Request[IO](), Response[IO](Status.NotFound)).unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote client responds with status different than OK or NOT_FOUND" in new TestCase {
      intercept[Exception] {
        mapResponse(Status.Unauthorized, Request[IO](), Response[IO]())
      }
    }
  }

  private trait TestCase {

    val creatorId = personGitLabIds.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    implicit val logger:       TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new GLCreatorFinderImpl[IO]

    lazy val mapResponse = captureMapping(finder, gitLabClient)(
      _.findCreatorName(creatorId).unsafeRunSync(),
      personNames.toGeneratorOfOptions
    )
  }

  private lazy val endpointName: String Refined NonEmpty = "single-user"
  private def uri(creatorId: persons.GitLabId) = uri"users" / creatorId
}
