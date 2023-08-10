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

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.RenkuTinyTypeGenerators.{cliVersions, projectSchemaVersions}
import org.http4s.Uri
import org.scalacheck.Gen

object Generators {

  def resultSuccesses[T](payloadGen: Gen[T]): Gen[Result[T]] =
    payloadGen.map(Result.success)

  implicit lazy val resultDetailedFailures: Gen[Result.Failure.Detailed] =
    (positiveInts().map(_.value) -> sentences().map(_.value))
      .mapN(Result.Failure.Detailed(_, _))

  implicit lazy val apiVersions: Gen[ApiVersion] =
    (positiveInts(), positiveInts()).mapN((major, minor) => ApiVersion(s"$major.$minor"))

  implicit lazy val coreUrisForSchema: Gen[RenkuCoreUri.ForSchema] =
    for {
      baseUri <- httpUrls()
      schema  <- projectSchemaVersions
    } yield RenkuCoreUri.ForSchema(Uri.unsafeFromString(baseUri), schema)

  implicit lazy val coreUrisVersioned: Gen[RenkuCoreUri.Versioned] =
    for {
      baseUri    <- coreUrisForSchema
      apiVersion <- apiVersions
    } yield RenkuCoreUri.Versioned(baseUri, apiVersion)

  implicit lazy val schemaApiVersions: Gen[SchemaApiVersions] =
    (apiVersions, apiVersions, cliVersions).mapN(SchemaApiVersions.apply)
}