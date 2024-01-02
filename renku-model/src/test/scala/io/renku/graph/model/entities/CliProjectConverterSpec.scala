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

package io.renku.graph.model.entities

import io.renku.cli.model.CliProject
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class CliProjectConverterSpec extends AnyFlatSpec with should.Matchers with EitherValues {

  it should "take the max of GL and CLI Project's dateModified" in {

    val glProject   = gitLabProjectInfos.generateOne
    val testProject = projectEntities(anyVisibility, cliShapedPersons).generateOne
    val cliProject  = testProject.to[CliProject]

    CliProjectConverter
      .fromCli(cliProject, allPersons = Set.empty, glProject)
      .toEither
      .value
      .dateModified shouldBe List(glProject.dateModified, cliProject.dateModified).max
  }
}
