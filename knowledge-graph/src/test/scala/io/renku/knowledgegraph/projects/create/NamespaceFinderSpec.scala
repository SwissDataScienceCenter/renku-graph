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

package io.renku.knowledgegraph.projects.create

import Generators.{namespaceIds, namespaces}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.GitLabClientTools
import org.http4s.Status.{Forbidden, NotFound, Ok}
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class NamespaceFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues
    with RenkuEntityCodec
    with GitLabClientTools[IO] {

  it should "call GL's GET gl/namespaces/:id and return Namespace.WithName " +
    "if GL responds with OK" in {

      val namespace   = namespaces.generateOne
      val accessToken = accessTokens.generateOne

      givenFindNamespace(
        namespace.identifier,
        accessToken,
        returning = Option(namespace).pure[IO]
      )

      finder.findNamespace(namespace.identifier, accessToken).asserting(_ shouldBe namespace.some)
    }

  it should "call GL's GET gl/namespaces/:id and return None " +
    "if no namespace is found" in {

      val namespace   = namespaces.generateOne
      val accessToken = accessTokens.generateOne

      givenFindNamespace(
        namespace.identifier,
        accessToken,
        returning = Option.empty.pure[IO]
      )

      finder.findNamespace(namespace.identifier, accessToken).asserting(_ shouldBe None)
    }

  it should "return a Namespace.WithName if GL returns 200 with the namespace details" in {
    val namespace = namespaces.generateOne
    mapResponse(Ok, Request[IO](), Response[IO](Ok).withEntity(namespace.asJson))
      .asserting(_ shouldBe namespace.some)
  }

  NotFound :: Forbidden :: Nil foreach { status =>
    it should s"return None if GL returns $status" in {
      mapResponse(status, Request[IO](), Response[IO](status)).asserting(_ shouldBe None)
    }
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new NamespaceFinderImpl[IO]

  private def givenFindNamespace(id:          NamespaceId,
                                 accessToken: AccessToken,
                                 returning:   IO[Option[Namespace.WithName]]
  ) = {
    val endpointName: String Refined NonEmpty = "namespace-details"
    (glClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[Namespace.WithName]])(
        _: Option[AccessToken]
      ))
      .expects(uri"namespaces" / id, endpointName, *, accessToken.some)
      .returning(returning)
  }

  private lazy val mapResponse: ResponseMappingF[IO, Option[Namespace.WithName]] =
    captureMapping(glClient)(
      finder
        .findNamespace(namespaceIds.generateOne, accessTokens.generateOne)
        .unsafeRunSync(),
      namespaces.toGeneratorOfOptions,
      underlyingMethod = Get
    )

  private implicit lazy val itemEncoder: Encoder[Namespace.WithName] = Encoder.instance {
    case Namespace.WithName(id, name) => json"""{
      "id":        $id,
      "full_path": $name
    }"""
  }
}
