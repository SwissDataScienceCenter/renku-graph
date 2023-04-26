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

package io.renku.graph.http.server.security

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
import io.renku.generators.Generators.{fixed, positiveInts}
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.persons
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.implicits._
import org.http4s.{Header, Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci._

class MembersFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO] {

  "findMembers" should {

    "return all project members" in new TestCase {

      val members = personGitLabIds.generateSet()
      givenFindingMembers(maybePage = None, returning = (members -> Option.empty[Int]).pure[IO])

      finder.findMembers(projectPath).unsafeRunSync() shouldBe members
    }

    "return project members from all pages" in new TestCase {

      val membersPage1 = personGitLabIds.generateSet()
      givenFindingMembers(maybePage = None, returning = (membersPage1 -> 2.some).pure[IO])
      val membersPage2 = personGitLabIds.generateSet()
      givenFindingMembers(maybePage = 2.some, returning = (membersPage2 -> Option.empty[Int]).pure[IO])

      finder.findMembers(projectPath).unsafeRunSync() shouldBe membersPage1 ++ membersPage2
    }

    "return members if OK" in new TestCase {

      val members = personGitLabIds.generateSet()

      mapResponse(Status.Ok, Request(), Response().withEntity(members.map(_.asJson(encoder)).toList.asJson))
        .unsafeRunSync() shouldBe (members, None)
    }

    "return members and the next page if OK" in new TestCase {

      val members  = personGitLabIds.generateSet()
      val nextPage = positiveInts().generateOne.value

      mapResponse(
        Status.Ok,
        Request(),
        Response[IO]()
          .withEntity(members.map(_.asJson(encoder)).toList.asJson)
          .withHeaders(Header.Raw(ci"X-Next-Page", nextPage.toString))
      ).unsafeRunSync() shouldBe (members, nextPage.some)
    }

    Status.NotFound :: Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
      s"return an empty Set for $status" in new TestCase {
        mapResponse(status, Request(), Response()).unsafeRunSync() shouldBe (Set.empty, None)
      }
    }
  }

  private trait TestCase {

    val projectPath = projectPaths.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient:   GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new MembersFinderImpl[IO]

    def givenFindingMembers(maybePage: Option[Int], returning: IO[(Set[persons.GitLabId], Option[Int])]) = {
      val endpointName: String Refined NonEmpty = "project-members"

      val uri = {
        val uri = uri"projects" / projectPath / "members"
        maybePage match {
          case Some(page) => uri withQueryParam ("page", page.toString)
          case None       => uri
        }
      }

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, (Set[persons.GitLabId], Option[Int])])(
          _: Option[AccessToken]
        ))
        .expects(uri, endpointName, *, maybeAccessToken)
        .returning(returning)
    }

    val mapResponse =
      captureMapping(gitLabClient)(
        finder.findMembers(projectPath)(maybeAccessToken).unsafeRunSync(),
        fixed((Set.empty[persons.GitLabId], Option.empty[Int]))
      )
  }

  private lazy val encoder: Encoder[persons.GitLabId] = Encoder.instance { id =>
    json"""{"id": $id}"""
  }
}
