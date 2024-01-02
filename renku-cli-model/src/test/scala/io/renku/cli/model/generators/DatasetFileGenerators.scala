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

package io.renku.cli.model.generators

import io.renku.cli.model.CliDatasetFile
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

import java.time.Instant

trait DatasetFileGenerators {

  def datasetFileGen(minCreated: Instant): Gen[CliDatasetFile] =
    for {
      id          <- RenkuTinyTypeGenerators.partResourceIdGen
      external    <- RenkuTinyTypeGenerators.datasetPartExternals
      entity      <- EntityGenerators.entityGen
      created     <- RenkuTinyTypeGenerators.datasetCreatedDates(minCreated)
      source      <- RenkuTinyTypeGenerators.datasetPartSources.toGeneratorOfOptions
      invalidTime <- RenkuTinyTypeGenerators.invalidationTimes(created).toGeneratorOfOptions
    } yield CliDatasetFile(id, external, entity, created, source, invalidTime)
}

object DatasetFileGenerators extends DatasetFileGenerators
