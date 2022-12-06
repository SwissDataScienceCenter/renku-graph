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

package io.renku.tokenrepository.repository.creation

import Generators._
import TokenDates.ExpiryDate
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
import io.renku.generators.Generators.{emptyOptionOf, fixed, localDates, nonEmptyStrings}
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s._
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate.now
import java.time.{LocalDate, Period}

class RevokeCandidatesFinderSpec
    extends AnyWordSpec
    with MockFactory
    with GitLabClientTools[IO]
    with IOSpec
    with should.Matchers {

  "checkTokenDue" should {

    s"do GET projects/:id/access_tokens to find expired '$renkuTokenName' tokens" in new TestCase {

      val endpointName: String Refined NonEmpty = "project-access-tokens"

      val allTokens = List(
        tokenInfosWithExpiry(fixed(renkuTokenName), localDates(max = now() plus tokenDuePeriod)),
        tokenInfosWithExpiry(fixed(renkuTokenName), fixed(now() plus tokenDuePeriod)),
        tokenInfosWithoutExpiry(fixed(renkuTokenName)),
        tokenInfosWithExpiry(fixed(renkuTokenName), localDates(min = now() plus tokenDuePeriod plusDays 1)),
        tokenInfosWithExpiry(expiryDates = localDates(max = now() plus tokenDuePeriod)),
        tokenInfosWithExpiry()
      ).map(_.generateOne)

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, List[TokenInfo]])(_: Option[AccessToken]))
        .expects(uri"projects" / projectId.value / "access_tokens", endpointName, *, Option(accessToken))
        .returning(allTokens.pure[IO])

      finder.findTokensToRemove(projectId, accessToken).unsafeRunSync() shouldBe allTokens.take(3).map(_._1)
    }

    "map OK response body to TokenInfo tuples" in new TestCase {

      val tokens = Gen
        .oneOf(tokenInfosWithExpiry(), tokenInfosWithoutExpiry(fixed(renkuTokenName)))
        .generateList()

      mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(tokens.asJson))
        .unsafeRunSync() shouldBe tokens
    }

    Status.Unauthorized :: Status.Forbidden :: Status.NotFound :: Nil foreach { status =>
      s"map $status response to None" in new TestCase {
        mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync() shouldBe Nil
      }
    }
  }

  private type TokenInfo = (AccessTokenId, String, Option[ExpiryDate])

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val tokenDuePeriod = Period.ofDays(5)
    val finder         = new RevokeCandidatesFinderImpl[IO](tokenDuePeriod)

    lazy val mapResponse = captureMapping(finder, gitLabClient)(
      findingMethod = _.findTokensToRemove(projectId, accessTokens.generateOne).unsafeRunSync(),
      resultGenerator = tokenInfosWithoutExpiry().generateList()
    )
  }

  private def tokenInfosWithoutExpiry(names: Gen[String] = nonEmptyStrings()): Gen[TokenInfo] =
    tokenInfos(names, maybeExpiryDates = emptyOptionOf[ExpiryDate])

  private def tokenInfosWithExpiry(names:       Gen[String] = nonEmptyStrings(),
                                   expiryDates: Gen[LocalDate] = localDates
  ): Gen[TokenInfo] = tokenInfos(names, expiryDates.toGeneratorOf(ExpiryDate).map(Option.apply))

  private def tokenInfos(names: Gen[String], maybeExpiryDates: Gen[Option[ExpiryDate]]): Gen[TokenInfo] =
    (accessTokenIds, names, maybeExpiryDates)
      .mapN { case (id, name, maybeExpiry) => (id, name, maybeExpiry) }

  private implicit lazy val tokenInfoEncoder: Encoder[TokenInfo] = Encoder.instance { case (id, name, expiry) =>
    json"""{
      "id":         $id,
      "name":       $name,
      "expires_at": $expiry
    }"""
  }
}
