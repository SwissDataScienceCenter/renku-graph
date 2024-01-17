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

package io.renku.tokenrepository.repository
package deletion

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.http.client.GitLabGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{fixed, nonEmptyStrings}
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.tokenrepository.repository.RepositoryGenerators.accessTokenIds
import org.http4s._
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci._

import scala.util.Random

class RevokeCandidatesFinderSpec
    extends AnyWordSpec
    with MockFactory
    with GitLabClientTools[IO]
    with IOSpec
    with should.Matchers {

  "projectAccessTokensStream" should {

    "call GET projects/:id/access_tokens to find environment related tokens" in new TestCase {

      val envTokens    = tokenInfos(names = fixed(renkuTokenName.value)).generateList(min = 1, max = pageSize.value - 1)
      val nonEnvTokens = tokenInfos().generateList(min = 1, max = pageSize.value - envTokens.size)
      val allTokens    = envTokens ::: nonEnvTokens

      fetchProjectTokens(Page(1), returning = (allTokens -> Option.empty[Page]).pure[IO])

      finder.projectAccessTokensStream(projectId, accessToken).compile.toList.unsafeRunSync() shouldBe
        envTokens.map(_._1)
    }

    "go through all the pages of project tokens" in new TestCase {

      val tokensToFind1 = tokenInfos(names = fixed(renkuTokenName.value)).generateOne
      val tokensToFind2 = tokenInfos(names = fixed(renkuTokenName.value)).generateOne

      val otherTokensPage1 =
        Random.shuffle(tokensToFind1 :: tokenInfos().generateFixedSizeList(ofSize = pageSize.value - 1))
      val otherTokensPage2 =
        Random.shuffle(tokensToFind2 :: tokenInfos().generateFixedSizeList(ofSize = pageSize.value - 1))

      fetchProjectTokens(Page(1), returning = (otherTokensPage1 -> Page(2).some).pure[IO])
      fetchProjectTokens(Page(2), returning = (otherTokensPage2 -> Option.empty[Page]).pure[IO])

      finder.projectAccessTokensStream(projectId, accessToken).compile.toList.unsafeRunSync() shouldBe
        List(tokensToFind1, tokensToFind2).map(_._1)
    }

    "map OK response body to TokenInfo tuples" in new TestCase {

      val tokens = Gen
        .oneOf(tokenInfos(), tokenInfos(names = fixed(renkuTokenName.value)))
        .generateList(max = pageSize.value)
      val nextPage = Page(2)

      mapResponse(
        Status.Ok,
        Request[IO](),
        Response[IO](Status.Ok)
          .withEntity(tokens.asJson)
          .withHeaders(Header.Raw(ci"X-Next-Page", nextPage.value.toString))
      ).unsafeRunSync() shouldBe (tokens -> nextPage.some)
    }

    Status.Unauthorized :: Status.Forbidden :: Status.NotFound :: Nil foreach { status =>
      s"map $status response to None" in new TestCase {
        mapResponse(status, Request[IO](), Response[IO](status))
          .unsafeRunSync() shouldBe (List.empty[TokenInfo] -> Option.empty[Page])
      }
    }
  }

  private type TokenInfo = (AccessTokenId, String)

  private trait TestCase extends RenkuEntityCodec {

    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    private implicit val gitLabClient: GitLabClient[IO]        = mock[GitLabClient[IO]]
    private val endpointName:          String Refined NonEmpty = "project-access-tokens"

    val pageSize       = PerPage(50)
    val renkuTokenName = nonEmptyStrings().generateAs(RenkuAccessTokenName(_))
    val finder         = new RevokeCandidatesFinderImpl[IO](pageSize, renkuTokenName)

    lazy val mapResponse = captureMapping(gitLabClient)(
      findingMethod = finder
        .projectAccessTokensStream(projectIds.generateOne, accessTokens.generateOne)
        .compile
        .toList
        .unsafeRunSync(),
      resultGenerator = tokenInfos().generateList() -> Option.empty[Page],
      underlyingMethod = Get
    )

    def fetchProjectTokens(page: Page, returning: IO[(List[TokenInfo], Option[Page])]) =
      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, (List[TokenInfo], Option[Page])])(
          _: Option[AccessToken]
        ))
        .expects((uri"projects" / projectId / "access_tokens")
                   .withQueryParam("per_page", pageSize)
                   .withQueryParam("page", page),
                 endpointName,
                 *,
                 accessToken.some
        )
        .returning(returning)
  }

  private def tokenInfos(names: Gen[String] = nonEmptyStrings()): Gen[TokenInfo] =
    (accessTokenIds, names).mapN(_ -> _)

  private implicit lazy val tokenInfoEncoder: Encoder[TokenInfo] = Encoder.instance { case (id, name) =>
    json"""{
      "id":   $id,
      "name": $name
    }"""
  }
}
