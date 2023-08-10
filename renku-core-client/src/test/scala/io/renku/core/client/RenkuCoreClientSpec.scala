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

import cats.effect.IO
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

  private implicit val logger: Logger[IO] = TestLogger()
  private val coreVersionClient = mock[RenkuCoreVersionClient[IO]]
  private lazy val client = new RenkuCoreClientImpl[IO](RenkuCoreUri.Current(externalServiceBaseUri), coreVersionClient)

//  private def givenCoreUriForSchemaInConfig(returning: RenkuCoreUri.ForSchema) =
//    (coreUriForSchemaLoader
//      .loadFromConfig[IO](_: SchemaVersion, _: Config)(_: MonadThrow[IO]))
//      .expects(returning.schemaVersion, config, *)
//      .returning(returning.pure[IO])
}
