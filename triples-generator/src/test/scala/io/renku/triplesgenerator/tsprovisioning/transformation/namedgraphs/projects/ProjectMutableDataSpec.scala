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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.projects

import TestDataTools.toProjectMutableData
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectMutableDataSpec extends AnyWordSpec with should.Matchers {

  "maybeMaxDateModified" should {

    "return max of all found dateModified if there are many" in {

      val project = anyProjectEntities
        .map(_.to[entities.Project])
        .map(toProjectMutableData)
        .generateOne

      val modifiedDates = projectModifiedDates(project.earliestDateCreated.value)
        .generateNonEmptyList()
        .toList

      project.copy(modifiedDates = modifiedDates).maybeMaxDateModified shouldBe Some(modifiedDates.max)
    }

    "return None if there are no dateModified" in {
      anyProjectEntities
        .map(_.to[entities.Project])
        .map(toProjectMutableData)
        .generateOne
        .copy(modifiedDates = Nil)
        .maybeMaxDateModified shouldBe None
    }
  }
}
