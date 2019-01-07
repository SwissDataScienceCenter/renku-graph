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

package ch.datascience.webhookservice.hookcreation

import org.scalatest.Matchers._
import org.scalatest.WordSpec
import ch.datascience.generators.Generators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.ProjectId

class ProjectIdPathBinderSpec extends WordSpec {

  private val binder = ProjectIdPathBinder.pathBinder

  "bind" should {

    "return right with the ProjectId instance for valid values" in {
      val projectId = projectIds.generateOne
      binder.bind( nonEmptyStrings().generateOne, projectId.toString ) shouldBe Right( projectId )
    }

    "return left with an error if the value does not meet requirements" in {
      val projectId = -1
      binder.bind( nonEmptyStrings().generateOne, projectId.toString ) shouldBe ProjectId.from( projectId )
    }
  }

  "unbind" should {

    "invoke toString on the given projectId" in {
      val projectId = projectIds.generateOne
      binder.unbind( nonEmptyStrings().generateOne, projectId ) shouldBe projectId.toString
    }
  }
}
