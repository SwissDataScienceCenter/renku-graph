/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.events

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

class ProjectSpec extends WordSpec with PropertyChecks {

  "instantiation" should {

    "be successful for non-negative ids and valid files" in {
      forAll(nonNegativeInts(), relativePaths) { (id, path) =>
        val project = Project(ProjectId(id), ProjectPath(path))
        project.id.value   shouldBe id
        project.path.value shouldBe path
      }
    }

    "fail for negative ids" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Project(ProjectId(-1), ProjectPath(relativePaths.generateOne))
      }
    }

    "fail for blank paths" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Project(ProjectId(nonNegativeInts().generateOne), ProjectPath(""))
      }
    }
  }
}
