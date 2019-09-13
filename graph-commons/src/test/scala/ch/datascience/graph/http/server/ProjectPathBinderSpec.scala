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

package ch.datascience.graph.http.server

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ProjectPathBinderSpec extends WordSpec {

  import binders.ProjectPath._

  "Namespace.unapply" should {

    "convert valid namespace as string to a Namespace" in {
      val namespaceValue = relativePaths(maxSegments = 1).generateOne

      val Some(namespace) = Namespace.unapply(namespaceValue)

      namespace       shouldBe a[Namespace]
      namespace.value shouldBe namespaceValue
    }

    "return None if string value cannot be converted to a Namespace" in {
      Namespace.unapply(blankStrings().generateOne) shouldBe None
    }
  }

  "Name.unapply" should {

    "convert valid project name as string to a Name" in {
      val nameValue = relativePaths(maxSegments = 1).generateOne

      val Some(name) = Name.unapply(nameValue)

      name       shouldBe a[Name]
      name.value shouldBe nameValue
    }

    "return None if string value cannot be converted to a Name" in {
      Name.unapply(blankStrings().generateOne) shouldBe None
    }
  }

  "Namespace /" should {

    import ch.datascience.graph.model.projects.{ProjectPath => ProjectPathType}

    "return a ProjectPath" in {
      val namespaceValue  = relativePaths(maxSegments = 1).generateOne
      val Some(namespace) = Namespace.unapply(namespaceValue)

      val nameValue  = relativePaths(maxSegments = 1).generateOne
      val Some(name) = Name.unapply(nameValue)

      namespace / name shouldBe ProjectPathType(s"$namespace/$name")
    }
  }
}
