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

package io.renku.triplesgenerator.generators

import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.versions.RenkuVersionPair
import io.renku.triplesgenerator.config.{RenkuPythonDevVersion, VersionCompatibilityConfig}
import org.scalacheck.Gen

object VersionGenerators {

  implicit val renkuVersionPairs: Gen[RenkuVersionPair] = for {
    cliVersion    <- cliVersions
    schemaVersion <- projectSchemaVersions
  } yield RenkuVersionPair(cliVersion, schemaVersion)

  val compatibilityGen: Gen[VersionCompatibilityConfig] =
    for {
      cliVersion      <- cliVersions
      schemaVersion   <- projectSchemaVersions
      renkuDevVersion <- Gen.option(cliVersions.map(v => RenkuPythonDevVersion(v.value)))
      reprov          <- Gen.oneOf(true, false)
    } yield VersionCompatibilityConfig(cliVersion, renkuDevVersion, schemaVersion, reprov)
}
