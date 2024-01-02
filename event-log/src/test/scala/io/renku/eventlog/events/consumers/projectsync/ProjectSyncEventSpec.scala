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

package io.renku.eventlog.events.consumers.projectsync

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectSyncEventSpec extends AnyWordSpec with should.Matchers {

  "show" should {
    "return String representation of the underlying Project" in {
      val id   = projectIds.generateOne
      val slug = projectSlugs.generateOne

      ProjectSyncEvent(id, slug).show shouldBe show"projectId = $id, projectSlug = $slug"
    }
  }
}
