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

package io.renku.tokenrepository.repository.creation

import Generators._
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Method.DELETE
import org.http4s._
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TokensRevokerSpec
    extends AnyWordSpec
    with MockFactory
    with GitLabClientTools[IO]
    with IOSpec
    with should.Matchers {

  "revokeToken" should {

    "do DELETE projects/:id/access_tokens/:token_id to revoke the given token" in new TestCase {

      val endpointName: String Refined NonEmpty = "revoke-project-access-token"

      (gitLabClient
        .delete(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Unit])(_: Option[AccessToken]))
        .expects(uri"projects" / projectId / "access_tokens" / tokenId, endpointName, *, Option(accessToken))
        .returning(().pure[IO])

      tokensRevoker.revokeToken(projectId, tokenId, accessToken).unsafeRunSync() shouldBe ()
    }

    Status.Ok :: Status.NoContent :: Status.Unauthorized :: Status.Forbidden :: Status.NotFound :: Nil foreach {
      status =>
        s"map $status to Unit" in new TestCase {
          mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync() shouldBe ()
        }
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val tokenId     = accessTokenIds.generateOne
    val accessToken = accessTokens.generateOne

    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val tokensRevoker = new TokensRevokerImpl[IO]

    lazy val mapResponse = captureMapping(tokensRevoker, gitLabClient)(
      findingMethod = _.revokeToken(projectId, tokenId, accessTokens.generateOne).unsafeRunSync(),
      resultGenerator = fixed(()),
      method = DELETE
    )
  }
}
