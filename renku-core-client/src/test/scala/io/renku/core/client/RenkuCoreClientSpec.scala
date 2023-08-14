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
import cats.MonadThrow
import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectSchemaVersions
import io.renku.graph.model.versions.SchemaVersion
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.CustomAsyncIOSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{EitherValues, OptionValues}
import org.typelevel.log4cats.Logger

class RenkuCoreClientSpec
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

        val coreUriForSchema = coreUrisForSchema.generateOne
        val apiVersions      = schemaApiVersions.generateOne

        givenCoreUriForSchemaInConfig(returning = coreUriForSchema)
        givenApiVersionFetching(coreUriForSchema, returning = Result.success(apiVersions))

        client
          .findCoreUri(coreUriForSchema.schemaVersion)
          .asserting(_ shouldBe Result.success(RenkuCoreUri.Versioned(coreUriForSchema, apiVersions.max)))
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

      val coreUriForSchema = coreUrisForSchema.generateOne
      val failure          = resultDetailedFailures.generateOne

      givenCoreUriForSchemaInConfig(returning = coreUriForSchema)
      givenApiVersionFetching(coreUriForSchema, returning = failure)

      client.findCoreUri(coreUriForSchema.schemaVersion).asserting(_ shouldBe failure)
    }
  }

  private implicit val logger: Logger[IO] = TestLogger()
  private val coreUriForSchemaLoader = mock[RenkuCoreUri.ForSchemaLoader]
  private val lowLevelApis           = mock[LowLevelApis[IO]]
  private lazy val config            = mock[Config]
  private lazy val client = new RenkuCoreClientImpl[IO](RenkuCoreUri.Current(externalServiceBaseUri),
                                                        coreUriForSchemaLoader,
                                                        lowLevelApis,
                                                        config
  )

  private def givenApiVersionFetching(coreUri: RenkuCoreUri.ForSchema, returning: Result[SchemaApiVersions]) =
    (lowLevelApis.getApiVersion _)
      .expects(coreUri)
      .returning(returning.pure[IO])

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
}
