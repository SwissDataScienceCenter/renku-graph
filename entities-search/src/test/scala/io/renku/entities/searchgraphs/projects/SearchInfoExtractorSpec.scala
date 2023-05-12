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

package io.renku.entities.searchgraphs.projects

import io.renku.entities.searchgraphs.PersonInfo
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SearchInfoExtractorSpec extends AnyWordSpec with should.Matchers {

  "extractSearchInfo" should {

    "convert the given non-modified Datasets to SearchInfo objects" in {

      val project = anyProjectEntities.generateOne.to[entities.Project]

      SearchInfoExtractor.extractSearchInfo(project) shouldBe ProjectSearchInfo(
        project.resourceId,
        project.name,
        project.path,
        project.visibility,
        project.dateCreated,
        project.maybeCreator.map(PersonInfo.toPersonInfo),
        project.keywords.toList,
        project.maybeDescription,
        project.images
      )
    }
  }
}
