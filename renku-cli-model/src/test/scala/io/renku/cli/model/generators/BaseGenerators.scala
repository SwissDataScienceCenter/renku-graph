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

package io.renku.cli.model.generators

import io.renku.cli.model.{CliDatasetSameAs, DateModified, EntityPath}
import io.renku.generators.Generators
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

import java.time.Instant

trait BaseGenerators {
  val entityPathGen: Gen[EntityPath] =
    Generators.relativePaths().map(EntityPath)

  def dateModifiedGen(min: Instant, max: Instant): Gen[DateModified] =
    Gen.choose(min, max).map(DateModified)

  val dateModified: Gen[DateModified] =
    dateModifiedGen(min = Instant.EPOCH, max = Instant.now())

  val datasetSameAs: Gen[CliDatasetSameAs] =
    RenkuTinyTypeGenerators.datasetSameAs.map(v => CliDatasetSameAs(v.value))
}

object BaseGenerators extends BaseGenerators
