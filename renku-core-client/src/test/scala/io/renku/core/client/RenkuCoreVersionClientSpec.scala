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

package io.renku.core.client

import Generators._
import ModelEncoders._
import cats.MonadThrow
import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import com.typesafe.config.Config
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.{cliVersions, projectSchemaVersions}
import io.renku.graph.model.versions.SchemaVersion
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.CustomAsyncIOSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{EitherValues, OptionValues}
import org.typelevel.log4cats.Logger

class RenkuCoreVersionClientSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with OptionValues
    with EitherValues
    with ExternalServiceStubbing
    with AsyncMockFactory {

  "findCoreUri" should {

    "find the uri of the core for the given schema in the config, " +
      "fetch the api version using the uri and " +
      "return the CoreUri relevant for the given schema" in {

        val schemaVersion = projectSchemaVersions.generateOne
        val apiVersions   = schemaApiVersions.generateOne

        givenApiVersionFetching(schemaVersion, returning = Result.success(apiVersions)).use {
          case (coreUriForSchema, _) =>
            givenCoreUriForSchemaInConfig(returning = coreUriForSchema)

            client
              .findCoreUri(schemaVersion)
              .asserting(_ shouldBe Result.success(RenkuCoreUri.Versioned(coreUriForSchema, apiVersions.max)))
        }
      }

    "fail if finding the uri of the core for the given schema in the config fails " in {

      val exception = exceptions.generateOne
      givenCoreUriForSchemaInConfig(failsWith = exception)

      val schemaVersion = projectSchemaVersions.generateOne

      client
        .findCoreUri(schemaVersion)
        .assertThrowsError[Exception](_ shouldBe exception)
    }

    "return a failure if fetching the api version fails" in {

      val schemaVersion = projectSchemaVersions.generateOne
      val failure       = resultDetailedFailures.generateOne

      givenApiVersionFetching(schemaVersion, returning = failure).use { case (coreUriForSchema, _) =>
        givenCoreUriForSchemaInConfig(returning = coreUriForSchema)

        client.findCoreUri(schemaVersion).asserting(_ shouldBe failure)
      }
    }
  }

  "getVersions" should {

    "return info about available versions" in {

      val versionTuples = (projectSchemaVersions -> cliVersions).mapN(_ -> _).generateList()

      stubFor {
        get(s"/renku/versions")
          .willReturn(ok(Result.success(versionTuples).asJson.spaces2))
      }

      client.getVersions.asserting(_ shouldBe Result.success(versionTuples.map(_._1)))
    }
  }

  "getApiVersion" should {

    "return info about API versions" in {

      val successResult = resultSuccesses(schemaApiVersions).generateOne

      givenApiVersionFetching(projectSchemaVersions.generateOne, returning = successResult).map {
        case (_, actualApiVersions) => actualApiVersions shouldBe successResult
      }.use_
    }
  }

  private val coreUriForSchemaLoader = mock[RenkuCoreUri.ForSchemaLoader]
  private lazy val config            = mock[Config]
  private implicit val logger: Logger[IO] = TestLogger()
  private lazy val client =
    new RenkuCoreVersionClientImpl[IO](RenkuCoreUri.Current(externalServiceBaseUri),
                                       coreUriForSchemaLoader,
                                       config,
                                       ClientTools[IO]
    )

  private def givenCoreUriForSchemaInConfig(returning: RenkuCoreUri.ForSchema) =
    (coreUriForSchemaLoader
      .loadFromConfig[IO](_: SchemaVersion, _: Config)(_: MonadThrow[IO]))
      .expects(returning.schemaVersion, config, *)
      .returning(returning.pure[IO])

  private def givenCoreUriForSchemaInConfig(failsWith: Throwable) =
    (coreUriForSchemaLoader
      .loadFromConfig[IO](_: SchemaVersion, _: Config)(_: MonadThrow[IO]))
      .expects(*, config, *)
      .returning(failsWith.raiseError[IO, Nothing])

  private def givenApiVersionFetching(schemaVersion: SchemaVersion, returning: Result[SchemaApiVersions]) =
    otherWireMockResource.evalMap { server =>
      server.stubFor {
        get(s"/renku/apiversion")
          .willReturn(ok(returning.asJson.spaces2))
      }

      val uriForSchema = RenkuCoreUri.ForSchema(server.baseUri, schemaVersion)

      client.getApiVersion(uriForSchema).map(uriForSchema -> _)
    }

  private implicit def resultEncoder[T](implicit enc: Encoder[T]): Encoder[Result[T]] =
    Encoder.instance {
      case Result.Success(obj) => json"""{
          "result": ${obj.asJson}
        }"""
      case Result.Failure.Detailed(code, userMessage) => json"""{
          "error": {
            "code":        $code,
            "userMessage": $userMessage
          }
        }"""
      case result => fail(s"$result shouldn't be in the core API response payload")
    }
}
