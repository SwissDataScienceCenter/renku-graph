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

package io.renku.cli.model
package generators

import io.renku.generators.Generators
import io.renku.graph.model.{RenkuTinyTypeGenerators, generations}
import org.scalacheck.Gen

trait EntityGenerators {

  def singleEntityGen: Gen[CliSingleEntity] = for {
    id       <- RenkuTinyTypeGenerators.entityResourceIds
    location <- BaseGenerators.entityPathGen
    checksum <- RenkuTinyTypeGenerators.entityChecksums
    genIds   <- Generators.listOf(RenkuTinyTypeGenerators.generationsResourceIdGen)
  } yield CliSingleEntity(id, location, checksum, genIds)

  def collectionEntityGen: Gen[CliCollectionEntity] = for {
    id       <- RenkuTinyTypeGenerators.entityResourceIds
    location <- BaseGenerators.entityPathGen
    checksum <- RenkuTinyTypeGenerators.entityChecksums
    genIds   <- Generators.listOf(RenkuTinyTypeGenerators.generationsResourceIdGen)
  } yield CliCollectionEntity(id, location, checksum, genIds)

  val entityGen: Gen[CliEntity] = Gen.oneOf(
    EntityGenerators.singleEntityGen.map(CliEntity.apply),
    EntityGenerators.collectionEntityGen.map(CliEntity.apply)
  )

  def entityGen(generationId: generations.ResourceId): Gen[CliEntity] = Gen.oneOf(
    EntityGenerators.singleEntityGen
      .map(_.copy(generationIds = List(generationId)))
      .map(CliEntity.apply),
    EntityGenerators.collectionEntityGen
      .map(_.copy(generationIds = List(generationId)))
      .map(CliEntity.apply)
  )
}

object EntityGenerators extends EntityGenerators
