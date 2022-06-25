/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.ConditionedMigration.MigrationRequired
import org.scalacheck.Gen

private object Generators {
  val migrationNames:       Gen[Migration.Name]    = nonEmptyStrings().toGeneratorOf(Migration.Name)
  val migrationRequiredYes: Gen[MigrationRequired] = nonEmptyStrings().map(MigrationRequired.Yes)
  val migrationRequiredNo:  Gen[MigrationRequired] = nonEmptyStrings().map(MigrationRequired.No)
  val migrationRequired:    Gen[MigrationRequired] = Gen.oneOf(migrationRequiredYes, migrationRequiredNo)
}
